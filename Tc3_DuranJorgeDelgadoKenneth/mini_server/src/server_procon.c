
#include "../includes/net_util.h"
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define _POSIX_C_SOURCE 200809L

#define DEFAULT_PORT 8080
#define LISTEN_BACKLOG 64
#define DRAIN_SECONDS 1
#define Consumers 4
#define Capacidad_Cola 64

static volatile sig_atomic_t g_running = 1;

static unsigned long g_requests_served = 0;//variable global para contar el número de solicitudes atendidas
//es posible al ser compartida por varios hilos, que se produzcan condiciones de carrera al actualizarla
//lo que podría dar lugar a resultados incorrectos o inconsistentes. Usare mutex para corregirlo

pthread_mutex_t candado = PTHREAD_MUTEX_INITIALIZER;//inicializo el mutex para proteger la variable global g_requests_served
//se hace global para que pueda ser accedida por todos los hilos


typedef struct {
    int file_descriptor;
    unsigned long connection_id;
} connection_t;

/* Estructura para la cola, producer metera conexiones que acepta y los consumers las sacan*/
typedef struct {
    connection_t items[Capacidad_Cola];
    size_t head;
    size_t tail;
    size_t count;
    int closed;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty; //despiertar a los consumers
    pthread_cond_t not_full;//despertar al producer
} cola_conexiones_t;


/* Se crea la Cola global compartida por todos los hilos */
static cola_conexiones_t g_cola = {
    .head = 0,
    .tail = 0,
    .count = 0,
    .closed = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .not_empty = PTHREAD_COND_INITIALIZER,
    .not_full = PTHREAD_COND_INITIALIZER
};
static void on_sigint(int signum)
{
    (void)signum;
    g_running = 0;
}

static int install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;

    if (sigaction(SIGINT, &sa, NULL) < 0)
    {
        perror("sigaction");
        return -1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;

    if (sigaction(SIGPIPE, &sa, NULL) < 0)
    {
        perror("sigaction");
        return -1;
    }

    return 0;
}
//ingresar conexion para consumers, esperar si esta llena
static int meter_conexion_cola(connection_t connection)
{
    pthread_mutex_lock(&g_cola.mutex);

    while (g_cola.count == Capacidad_Cola && !g_cola.closed) {
        pthread_cond_wait(&g_cola.not_full, &g_cola.mutex);
    }

    if (g_cola.closed) {
        pthread_mutex_unlock(&g_cola.mutex);
        return -1;
    }

    g_cola.items[g_cola.tail] = connection;
    g_cola.tail = (g_cola.tail + 1) % Capacidad_Cola;
    ++g_cola.count;

    pthread_cond_signal(&g_cola.not_empty);
    pthread_mutex_unlock(&g_cola.mutex);
    return 0;
}

// funcion para los consumer, sacan una conexion y esperan si esta vacia
static int sacar_conexion_cola(connection_t *connection)
{
    pthread_mutex_lock(&g_cola.mutex);

    while (g_cola.count == 0 && !g_cola.closed) {
        pthread_cond_wait(&g_cola.not_empty, &g_cola.mutex);
    }

    if (g_cola.count == 0 && g_cola.closed) {
        pthread_mutex_unlock(&g_cola.mutex);
        return -1;
    }

    *connection = g_cola.items[g_cola.head];
    g_cola.head = (g_cola.head + 1) % Capacidad_Cola;
    --g_cola.count;

    pthread_cond_signal(&g_cola.not_full);
    pthread_mutex_unlock(&g_cola.mutex);
    return 0;
}

/* Cierra la cola y despierta a todos los hilos para que terminen */
static void cerrar_cola(void)
{
    pthread_mutex_lock(&g_cola.mutex);
    g_cola.closed = 1;
    pthread_cond_broadcast(&g_cola.not_empty);
    pthread_cond_broadcast(&g_cola.not_full);
    pthread_mutex_unlock(&g_cola.mutex);
}
/* Hilo consumidor: atiende conexiones de la cola hasta que se cierre */
static void *consumer(void *arg)
{
long consumer_id = (long)arg;
connection_t connection;
while (sacar_conexion_cola(&connection) == 0) {
printf("[Consumidor %ld] atendiendo conexion %lu\n",
consumer_id, connection.connection_id);
fflush(stdout);
if (nu_drain_request(connection.file_descriptor) >= 0) {
(void)nu_send_response(connection.file_descriptor,
connection.connection_id);
}
if (close(connection.file_descriptor) < 0) {
perror("close(file_descriptor)");
}
pthread_mutex_lock(&candado);
++g_requests_served;
pthread_mutex_unlock(&candado);
}
return NULL;
}



static unsigned short parse_port(int argc, char **argv)
{
    if (argc < 2)
    {
        return DEFAULT_PORT;
    }

    char *end = NULL;
    errno = 0;
    long value = strtol(argv[1], &end, 10);

    if (errno != 0 || end == argv[1] || *end != '\0' ||
        value <= 0 || value > 65535) {
        fprintf(stderr, "invalid port '%s', using %d\n", argv[1], DEFAULT_PORT);
        return DEFAULT_PORT;
        }

    return (unsigned short)value;
}

// Lee la cantidad de hilos desde argv[2] o usa el valor por defecto
static long parse_consumers(int argc, char **argv)
{
if (argc < 3) {
return Consumers;
}
char *end = NULL;
errno = 0;
long value = strtol(argv[2], &end, 10);
if (errno != 0 || end == argv[2] || *end != '\0' || value <= 0) {
fprintf(stderr, "cantidad de consumidores invalida '%s', usando %d\n",
argv[2], Consumers);
return Consumers;
}
return value;
}

int main(int argc, char **argv)
{
if (install_signal_handlers() < 0) {
return EXIT_FAILURE;
}
unsigned short port = parse_port(argc, argv);
long consumer_count = parse_consumers(argc, argv);
int listen_file_descriptor = nu_listen(port, LISTEN_BACKLOG);
if (listen_file_descriptor < 0) {
return EXIT_FAILURE;
}
pthread_t *consumers = calloc((size_t)consumer_count, sizeof(*consumers));
if (consumers == NULL) {
fprintf(stderr, "sin memoria\n");
close(listen_file_descriptor);
return EXIT_FAILURE;
}
long started = 0;
for (long i = 0; i < consumer_count; ++i) {
int pthread_created = pthread_create(&consumers[i], NULL,
consumer, (void *)(i + 1));
if (pthread_created != 0) {
fprintf(stderr, "pthread_create fallo: %s\n",
strerror(pthread_created));
break;
}
++started;
}
printf("servidor productor-consumidor esperando en puerto %u con %ld consumidor(es) "
"— Ctrl-C para detener\n",
port, started);
fflush(stdout);
unsigned long accepted = 0;
while (g_running) {
int client_file_descriptor = accept(listen_file_descriptor, NULL, NULL);
if (client_file_descriptor < 0) {
if (errno == EINTR) {
continue;
}
perror("accept");
break;
}
connection_t connection;
connection.file_descriptor = client_file_descriptor;
connection.connection_id = ++accepted;
if (meter_conexion_cola(connection) < 0) {
close(client_file_descriptor);
break;
}
}
if (close(listen_file_descriptor) < 0) {
perror("close(listen_file_descriptor)");
}
cerrar_cola();
for (long i = 0; i < started; ++i) {
int rc = pthread_join(consumers[i], NULL);
if (rc != 0) {
fprintf(stderr, "pthread_join: %s\n", strerror(rc));
}
}
free(consumers);
sleep(DRAIN_SECONDS);
pthread_mutex_lock(&candado);
unsigned long served = g_requests_served;
pthread_mutex_unlock(&candado);
printf("\naceptadas: %lu\n", accepted);
printf("atendidas: %lu\n", served);
printf("perdidas: %ld\n", (long)accepted - (long)served);
return EXIT_SUCCESS;
}

