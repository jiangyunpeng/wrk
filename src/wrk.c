// Copyright (C) 2012 - Will Glozer.  All rights reserved.

#include "wrk.h"
#include "script.h"
#include "main.h"

static struct config {
    uint64_t connections;
    uint64_t duration;
    uint64_t threads;
    uint64_t timeout;
    uint64_t pipeline;
    bool     delay;
    bool     dynamic;
    bool     latency;
    char    *host;
    char    *script;
    SSL_CTX *ctx;
} cfg;

static struct {
    stats *latency;
    stats *requests;
} statistics;

static struct sock sock = {
    .connect  = sock_connect,
    .close    = sock_close,
    .read     = sock_read,
    .write    = sock_write,
    .readable = sock_readable
};

static struct http_parser_settings parser_settings = {
    .on_message_complete = response_complete
};

static volatile sig_atomic_t stop = 0;

static void handler(int sig) {
    stop = 1;
}

static void usage() {
    printf("Usage: wrk <options> <url>                            \n"
           "  Options:                                            \n"
           "    -c, --connections <N>  Connections to keep open   \n"
           "    -d, --duration    <T>  Duration of test           \n"
           "    -t, --threads     <N>  Number of threads to use   \n"
           "                                                      \n"
           "    -s, --script      <S>  Load Lua script file       \n"
           "    -H, --header      <H>  Add header to request      \n"
           "        --latency          Print latency statistics   \n"
           "        --timeout     <T>  Socket/request timeout     \n"
           "    -v, --version          Print version details      \n"
           "                                                      \n"
           "  Numeric arguments may include a SI unit (1k, 1M, 1G)\n"
           "  Time arguments may include a time unit (2s, 2m, 2h)\n");
}

int main(int argc, char **argv) {
    char *url, **headers = zmalloc(argc * sizeof(char *));
    struct http_parser_url parts = {};

    if (parse_args(&cfg, &url, &parts, headers, argc, argv)) {
        usage();
        exit(1);
    }

    char *schema  = copy_url_part(url, &parts, UF_SCHEMA);
    char *host    = copy_url_part(url, &parts, UF_HOST);
    char *port    = copy_url_part(url, &parts, UF_PORT);
    char *service = port ? port : schema;

    if (!strncmp("https", schema, 5)) {
        if ((cfg.ctx = ssl_init()) == NULL) {
            fprintf(stderr, "unable to initialize SSL\n");
            ERR_print_errors_fp(stderr);
            exit(1);
        }
        sock.connect  = ssl_connect;
        sock.close    = ssl_close;
        sock.read     = ssl_read;
        sock.write    = ssl_write;
        sock.readable = ssl_readable;
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  SIG_IGN);

    statistics.latency  = stats_alloc(cfg.timeout * 1000);
    statistics.requests = stats_alloc(MAX_THREAD_RATE_S);
    thread *threads     = zcalloc(cfg.threads * sizeof(thread));

    lua_State *L = script_create(cfg.script, url, headers);

    //解析脚本，这里有点绕，就是把有些方法交给lua脚本执行，lua又会调用script.c(比如script_wrk_lookup)默认会执行wrk.lua
    if (!script_resolve(L, host, service)) {
        char *msg = strerror(errno);
        fprintf(stderr, "unable to connect to %s:%s %s\n", host, service, msg);
        exit(1);
    }

    cfg.host = host;

    //循环遍历线程
    for (uint64_t i = 0; i < cfg.threads; i++) {
        thread *t      = &threads[i];
        t->loop        = aeCreateEventLoop(10 + cfg.connections * 3); //创建事件循环
        t->connections = cfg.connections / cfg.threads; //总连接数平均分配到每个线程， 200/20=10

        t->L = script_create(cfg.script, url, headers);

        script_init(L, t, argc - optind, &argv[optind]);

        if (i == 0) { //仅对第一个线程进行特定的脚本验证和配置
            cfg.pipeline = script_verify_request(t->L); //默认为1
            cfg.dynamic  = !script_is_static(t->L); //默认false

            cfg.delay    = script_has_delay(t->L);
            if (script_want_response(t->L)) {
                parser_settings.on_header_field = header_field;
                parser_settings.on_header_value = header_value;
                parser_settings.on_body         = response_body; //解析内容的回调
            }
        }

        if (!t->loop || pthread_create(&t->thread, NULL, &thread_main, t)) { //创建线程,thread_main作为主函数
            char *msg = strerror(errno);
            fprintf(stderr, "unable to create thread %"PRIu64": %s\n", i, msg);
            exit(2);
        }
    }

    struct sigaction sa = {
        .sa_handler = handler,
        .sa_flags   = 0,
    };
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);   //关联中断信号，例如Ctrl+C

    char *time = format_time_s(cfg.duration);
    printf("Running %s test @ %s\n", time, url);
    printf("  %"PRIu64" threads and %"PRIu64" connections\n", cfg.threads, cfg.connections); //打印信息

    uint64_t start    = time_us();  //获取当前时间（微秒）
    uint64_t complete = 0;          //完成请求数
    uint64_t bytes    = 0;          //传输字节数
    errors errors     = { 0 };      //错误统计

    sleep(cfg.duration);//等待持续时间
    stop = 1;

    for (uint64_t i = 0; i < cfg.threads; i++) {
        thread *t = &threads[i];
        pthread_join(t->thread, NULL);

        complete += t->complete;
        bytes    += t->bytes;

        errors.connect += t->errors.connect;
        errors.read    += t->errors.read;
        errors.write   += t->errors.write;
        errors.timeout += t->errors.timeout;
        errors.status  += t->errors.status;
    }

    uint64_t runtime_us = time_us() - start;    //计算运行时间（微秒）
    long double runtime_s   = runtime_us / 1000000.0; //转换为秒
    long double req_per_s   = complete   / runtime_s;  //qps
    long double bytes_per_s = bytes      / runtime_s;  //每秒传输字节数

    //如果完成的请求数除以连接数大于0，计算每个连接的平均间隔时间，并修正延迟统计数据。
    if (complete / cfg.connections > 0) {
        int64_t interval = runtime_us / (complete / cfg.connections);
        stats_correct(statistics.latency, interval);
    }

    print_stats_header();
    print_stats("Latency", statistics.latency, format_time_us);
    print_stats("Req/Sec", statistics.requests, format_metric);
    if (cfg.latency) print_stats_latency(statistics.latency);

    char *runtime_msg = format_time_us(runtime_us);

    printf("  %"PRIu64" requests in %s, %sB read\n", complete, runtime_msg, format_binary(bytes));
    if (errors.connect || errors.read || errors.write || errors.timeout) {
        printf("  Socket errors: connect %d, read %d, write %d, timeout %d\n",
               errors.connect, errors.read, errors.write, errors.timeout);
    }

    if (errors.status) { //如果存在非2xx或3xx响应，打印这些响应的错误数
        printf("  Non-2xx or 3xx responses: %d\n", errors.status);
    }

    printf("Requests/sec: %9.2Lf\n", req_per_s);
    printf("Transfer/sec: %10sB\n", format_binary(bytes_per_s));

    if (script_has_done(L)) {
        script_summary(L, runtime_us, complete, bytes);
        script_errors(L, &errors);
        script_done(L, statistics.latency, statistics.requests);
    }

    return 0;
}

/**
 * 主要功能:
 * 1. 初始化每个线程的连接数组，并为每个连接设置必要的属性和建立连接。
 * 2. 设置一个定时事件用于记录性能数据。
 * 3. 进入事件循环处理事件。
 * 4. 事件循环结束后清理资源。
 */
void *thread_main(void *arg) {
    thread *thread = arg;

    //初始化 request 和 length，分别用于保存请求数据和其长度
    char *request = NULL;
    size_t length = 0;

    //如果配置不是动态请求, 调用 script_request 从脚本中获取请求数据并设置 request 和 length。
    if (!cfg.dynamic) {
        script_request(thread->L, &request, &length);
    }

    printf("=============================================\n");
    printf("AddByMe thread_main length=%zu , request= %s\n",length,request);
    printf("AddByMe cfg.dynamic %s \n",cfg.dynamic ? "true" : "false");
    printf("AddByMe main cfg.pending=%llu \n",cfg.pipeline);
    printf("=============================================\n");

    //使用 zcalloc 分配内存，为线程的所有连接分配空间
    thread->cs = zcalloc(thread->connections * sizeof(connection));
    connection *c = thread->cs;

    //遍历所有连接, 为每个连接 c 初始化以下属性：
    for (uint64_t i = 0; i < thread->connections; i++, c++) {
        c->thread = thread;
        c->ssl     = cfg.ctx ? SSL_new(cfg.ctx) : NULL;
        c->request = request;        //设置连接的请求数据
        c->length  = length;         //设置连接的请求数据长度
        c->delayed = cfg.delay;     //延迟标志，只有lua脚本才会用到
        connect_socket(thread, c); //建立连接
    }

    aeEventLoop *loop = thread->loop; //获取线程的事件循环 loop。
    // 创建一个定时事件，每100毫秒调用一次 record_rate 函数，内部会检测是否收到stop关闭
    aeCreateTimeEvent(loop, RECORD_INTERVAL_MS, record_rate, thread, NULL);

    thread->start = time_us(); //记录线程开始时间 thread->start
    aeMain(loop); //调用 aeMain 进入事件循环，开始处理事件

    aeDeleteEventLoop(loop); //事件循环结束后，删除事件循环 loop。
    zfree(thread->cs); //释放分配的连接数组内存 thread->cs。

    return NULL;
}

/**
* 这段代码定义了一个静态函数 connect_socket，用于初始化套接字并尝试建立与服务器的连接
**/
static int connect_socket(thread *thread, connection *c) {
    struct addrinfo *addr = thread->addr;
    struct aeEventLoop *loop = thread->loop;
    int fd, flags;

    //调用 socket 函数创建套接字，使用地址信息中的协议族、套接字类型和协议。
    fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol); //fd表示socket返回文件描述符

    flags = fcntl(fd, F_GETFL, 0); //使用 fcntl 获取套接字文件描述符的当前标志
    fcntl(fd, F_SETFL, flags | O_NONBLOCK); //使用 fcntl 设置套接字为非阻塞模式（O_NONBLOCK）

    //调用 connect 函数尝试连接服务器。
    if (connect(fd, addr->ai_addr, addr->ai_addrlen) == -1) {
        if (errno != EINPROGRESS) goto error;
    }

    //调用系统setsockopt()函数，通过定义sys/socket.h
    flags = 1; //表示开启
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));//设置禁用 Nagle 算法以减少延迟

    //在事件循环中注册可读和可写（AE_READABLE | AE_WRITABLE）事件，回调函数为 socket_connected
    flags = AE_READABLE | AE_WRITABLE;
    if (aeCreateFileEvent(loop, fd, flags, socket_connected, c) == AE_OK) {
        c->parser.data = c;
        c->fd = fd;
        return fd;
    }

  error:
    thread->errors.connect++;
    close(fd);
    return -1;
}

static int reconnect_socket(thread *thread, connection *c) {
    aeDeleteFileEvent(thread->loop, c->fd, AE_WRITABLE | AE_READABLE);
    sock.close(c);
    close(c->fd);
    return connect_socket(thread, c);
}

static int record_rate(aeEventLoop *loop, long long id, void *data) {
    thread *thread = data;

    if (thread->requests > 0) {
        uint64_t elapsed_ms = (time_us() - thread->start) / 1000;
        uint64_t requests = (thread->requests / (double) elapsed_ms) * 1000;

        stats_record(statistics.requests, requests);

        thread->requests = 0;
        thread->start    = time_us();
    }

    if (stop) aeStop(loop);

    return RECORD_INTERVAL_MS;
}

static int delay_request(aeEventLoop *loop, long long id, void *data) {
    connection *c = data;
    c->delayed = false;
    aeCreateFileEvent(loop, c->fd, AE_WRITABLE, socket_writeable, c);
    return AE_NOMORE;
}

static int header_field(http_parser *parser, const char *at, size_t len) {
    connection *c = parser->data;
    if (c->state == VALUE) {
        *c->headers.cursor++ = '\0';
        c->state = FIELD;
    }
    buffer_append(&c->headers, at, len);
    return 0;
}

static int header_value(http_parser *parser, const char *at, size_t len) {
    connection *c = parser->data;
    if (c->state == FIELD) {
        *c->headers.cursor++ = '\0';
        c->state = VALUE;
    }
    buffer_append(&c->headers, at, len);
    return 0;
}

static int response_body(http_parser *parser, const char *at, size_t len) {
    connection *c = parser->data;
    buffer_append(&c->body, at, len);
    return 0;
}

//用于处理 HTTP 响应完成后的操作。该函数在解析器 http_parser 完成一个 HTTP 响应时被调用
static int response_complete(http_parser *parser) {
    connection *c = parser->data;
    thread *thread = c->thread;
    uint64_t now = time_us();               //获取当前时间（微秒）
    int status = parser->status_code;       //从解析器获取响应状态码

    thread->complete++;                     //增加计数
    thread->requests++;

    if (status > 399) {                     //如果状态码大于 399，表示请求失败，增加线程的状态错误计数器
        thread->errors.status++;
    }

    if (c->headers.buffer) {                //处理头部和主体
        *c->headers.cursor++ = '\0';
        script_response(thread->L, status, &c->headers, &c->body);
        c->state = FIELD;
    }

    //printf("AddByMe response_complete pending=%llu \n",c->pending);

    if (--c->pending == 0) { // 如果待处理请求计数器为 0
        if (!stats_record(statistics.latency, now - c->start)) { //记录rt统计数据
            thread->errors.timeout++;
        }
        c->delayed = cfg.delay;
        //在事件循环中为连接的文件描述符创建可写事件 AE_WRITABLE，回调函数为 socket_writeable。
        aeCreateFileEvent(thread->loop, c->fd, AE_WRITABLE, socket_writeable, c);
    }

    if (!http_should_keep_alive(parser)) {
        reconnect_socket(thread, c);
        goto done;
    }

    http_parser_init(parser, HTTP_RESPONSE);

  done:
    return 0;
}

//注册事件成功之后的回调函数
//该函数主要完成连接的检查、初始化 HTTP 解析器、设置文件事件，以及在连接失败时重新连接。
static void socket_connected(aeEventLoop *loop, int fd, void *data, int mask) {
    connection *c = data;

    //调用sock连接
    switch (sock.connect(c, cfg.host)) {
        case OK:    break;
        case ERROR: goto error;
        case RETRY: return;
    }

    //初始化 HTTP 解析器，将其设置为解析 HTTP 响应。
    http_parser_init(&c->parser, HTTP_RESPONSE);
    //重置已写字节数为0，表示尚未写入任何数据。
    c->written = 0;

    //在事件循环中注册两个事件:
    aeCreateFileEvent(c->thread->loop, fd, AE_READABLE, socket_readable, c); //可读事件，回调函数:socket_readable
    aeCreateFileEvent(c->thread->loop, fd, AE_WRITABLE, socket_writeable, c); //可写事件，回调函数:socket_writeable

    return;

  error:
    c->thread->errors.connect++;
    reconnect_socket(c->thread, c);
}

//可写回调函数
static void socket_writeable(aeEventLoop *loop, int fd, void *data, int mask) {
    connection *c = data;
    thread *thread = c->thread;

    //如果连接 c 有延迟标志 c->delayed:
    // ①调用 script_delay 获取延迟时间，②删除当前的可写事件 AE_WRITABLE,创建一个定时事件，③延迟时间到达后调用 delay_request 函数
    if (c->delayed) {
        uint64_t delay = script_delay(thread->L);
        aeDeleteFileEvent(loop, fd, AE_WRITABLE);
        aeCreateTimeEvent(loop, delay, delay_request, c, NULL);
        return;
    }

    //written为0，表示还没有发送任何数据
    if (!c->written) {
        if (cfg.dynamic) { //如果是动态请求，从脚本获取数据
            script_request(thread->L, &c->request, &c->length);
        }
        c->start   = time_us(); //记录开始时间
        c->pending = cfg.pipeline; //设置待处理的请求数量
    }

    char  *buf = c->request + c->written; //计算要发送的数据缓冲区 buf,从请求的已写偏移量 c->written 开始
    size_t len = c->length  - c->written; //计算要发送的数据长度 len，为总长度减去已写入的字节数
    size_t n; //存储实际写入的字节数

    //发送数据
    switch (sock.write(c, buf, len, &n)) {
        case OK:    break;
        case ERROR: goto error;
        case RETRY: return;
    }

    //更新写入的字节数
    c->written += n;
    if (c->written == c->length) {
        c->written = 0;
        aeDeleteFileEvent(loop, fd, AE_WRITABLE);
    }

    return;

  error:
    thread->errors.write++;
    reconnect_socket(thread, c);
}

//该函数主要负责读取数据、解析 HTTP 响应并处理可能的错误。
static void socket_readable(aeEventLoop *loop, int fd, void *data, int mask) {
    connection *c = data;
    size_t n;

    do {//读取数据
        switch (sock.read(c, &n)) { //调用 sock.read 函数读取数据，并将读取的字节数存储在 n 中。
            case OK:    break;
            case ERROR: goto error;
            case RETRY: return;
        }
        //解析http
        if (http_parser_execute(&c->parser, &parser_settings, c->buf, n) != n) goto error;
        if (n == 0 && !http_body_is_final(&c->parser)) goto error; //失败进入错误

        c->thread->bytes += n; //增加线程的总字节数
    } while (n == RECVBUF && sock.readable(c) > 0);

    return;

  error:
    c->thread->errors.read++;
    reconnect_socket(c->thread, c);//错误处理，重连
}

static uint64_t time_us() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (t.tv_sec * 1000000) + t.tv_usec;
}

static char *copy_url_part(char *url, struct http_parser_url *parts, enum http_parser_url_fields field) {
    char *part = NULL;

    if (parts->field_set & (1 << field)) {
        uint16_t off = parts->field_data[field].off;
        uint16_t len = parts->field_data[field].len;
        part = zcalloc(len + 1 * sizeof(char));
        memcpy(part, &url[off], len);
    }

    return part;
}

static struct option longopts[] = {
    { "connections", required_argument, NULL, 'c' },
    { "duration",    required_argument, NULL, 'd' },
    { "threads",     required_argument, NULL, 't' },
    { "script",      required_argument, NULL, 's' },
    { "header",      required_argument, NULL, 'H' },
    { "latency",     no_argument,       NULL, 'L' },
    { "timeout",     required_argument, NULL, 'T' },
    { "help",        no_argument,       NULL, 'h' },
    { "version",     no_argument,       NULL, 'v' },
    { NULL,          0,                 NULL,  0  }
};

static int parse_args(struct config *cfg, char **url, struct http_parser_url *parts, char **headers, int argc, char **argv) {
    char **header = headers;
    int c;

    memset(cfg, 0, sizeof(struct config));
    cfg->threads     = 2;
    cfg->connections = 10;
    cfg->duration    = 10;
    cfg->timeout     = SOCKET_TIMEOUT_MS;

    while ((c = getopt_long(argc, argv, "t:c:d:s:H:T:Lrv?", longopts, NULL)) != -1) {
        switch (c) {
            case 't':
                if (scan_metric(optarg, &cfg->threads)) return -1;
                break;
            case 'c':
                if (scan_metric(optarg, &cfg->connections)) return -1;
                break;
            case 'd':
                if (scan_time(optarg, &cfg->duration)) return -1;
                break;
            case 's':
                cfg->script = optarg;
                break;
            case 'H':
                *header++ = optarg;
                break;
            case 'L':
                cfg->latency = true;
                break;
            case 'T':
                if (scan_time(optarg, &cfg->timeout)) return -1;
                cfg->timeout *= 1000;
                break;
            case 'v':
                printf("wrk %s [%s] ", VERSION, aeGetApiName());
                printf("Copyright (C) 2012 Will Glozer\n");
                break;
            case 'h':
            case '?':
            case ':':
            default:
                return -1;
        }
    }

    if (optind == argc || !cfg->threads || !cfg->duration) return -1;

    if (!script_parse_url(argv[optind], parts)) {
        fprintf(stderr, "invalid URL: %s\n", argv[optind]);
        return -1;
    }

    if (!cfg->connections || cfg->connections < cfg->threads) {
        fprintf(stderr, "number of connections must be >= threads\n");
        return -1;
    }

    *url    = argv[optind];
    *header = NULL;

    return 0;
}

static void print_stats_header() {
    printf("  Thread Stats%6s%11s%8s%12s\n", "Avg", "Stdev", "Max", "+/- Stdev");
}

static void print_units(long double n, char *(*fmt)(long double), int width) {
    char *msg = fmt(n);
    int len = strlen(msg), pad = 2;

    if (isalpha(msg[len-1])) pad--;
    if (isalpha(msg[len-2])) pad--;
    width -= pad;

    printf("%*.*s%.*s", width, width, msg, pad, "  ");

    free(msg);
}

static void print_stats(char *name, stats *stats, char *(*fmt)(long double)) {
    uint64_t max = stats->max;
    long double mean  = stats_mean(stats);
    long double stdev = stats_stdev(stats, mean);

    printf("    %-10s", name);
    print_units(mean,  fmt, 8);
    print_units(stdev, fmt, 10);
    print_units(max,   fmt, 9);
    printf("%8.2Lf%%\n", stats_within_stdev(stats, mean, stdev, 1));
}

static void print_stats_latency(stats *stats) {
    long double percentiles[] = { 50.0, 75.0, 90.0, 99.0 };
    printf("  Latency Distribution\n");
    for (size_t i = 0; i < sizeof(percentiles) / sizeof(long double); i++) {
        long double p = percentiles[i];
        uint64_t n = stats_percentile(stats, p);
        printf("%7.0Lf%%", p);
        print_units(n, format_time_us, 10);
        printf("\n");
    }
}
