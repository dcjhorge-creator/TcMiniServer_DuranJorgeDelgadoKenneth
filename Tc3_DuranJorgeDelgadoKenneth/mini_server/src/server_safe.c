

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

static void *handle_connection(void *arg)
{
    connection_t *conn = arg;

    printf("[Handling connection %lu] accepted\n", conn->connection_id);
    fflush(stdout);

    if (nu_drain_request(conn->file_descriptor) < 0)
    {
        (void)nu_send_response(conn->file_descriptor, conn->connection_id);
    }

    unsigned long current = g_requests_served;
    sched_yield();
    pthread_mutex_lock(&candado);//bloqueo el mutex para proteger la variable global g_requests_served
    g_requests_served = current + 1;//aqui esta la condición de carrera, ya que varios hilos pueden leer y escribir en g_requests_servedl mismo tiempo, lo que puede dar lugar a resultados incorrectos o inconsistentes. Para corregirlo, se debe usar un mutex para proteger el acceso a la variable global g_requests_served.
    pthread_mutex_unlock(&candado);//desbloqueo el mutex para permitir que otros hilos accedan a la variable global g_requests_served

    if (close(conn->file_descriptor) < 0)
        perror("close(file_descriptor)");

    free(conn);
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

int main(int argc, char **argv)
{
    if (install_signal_handlers() < 0)
    {
        return EXIT_FAILURE;
    }
    unsigned short port = parse_port(argc, argv);

    int listen_file_descriptor = nu_listen(port, LISTEN_BACKLOG);

    if (listen_file_descriptor < 0)
    {
        return EXIT_FAILURE;
    }

    printf("listening on port %u — Ctrl-C to stop\n", port);
    fflush(stdout);

    unsigned long accepted = 0;

    while (g_running)
    {
        int client_file_descriptor = accept(listen_file_descriptor, NULL, NULL);
        if (client_file_descriptor < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("accept");
            break;
        }
        connection_t *conn = malloc(sizeof(connection_t));

        if (conn == NULL)
        {
            fprintf(stderr, "out of memory, dropping connection\n");
            close(client_file_descriptor);
            continue;
        }

        conn->file_descriptor = client_file_descriptor;
        conn->connection_id = ++accepted;

        pthread_t thread_id;

        int pthread_created = pthread_create(&thread_id, NULL, handle_connection, conn);
        //vamos donde se crea el hilo y vemos la función handle_connection, que es la que se ejecutará en el hilo creado
        //ire a esa funcion para ver si hay alguna condición de carrera al actualizar la variable global g_requests_served

        if (pthread_created != 0)
        {
            fprintf(stderr, "pthread_create failed %s\n", strerror(pthread_created));
            close(client_file_descriptor);
            free(conn);
            --accepted;
            continue;
        }
        pthread_created = pthread_detach(thread_id);
        if (pthread_created != 0)
        {
            fprintf(stderr, "pthread_detach failed %s\n", strerror(pthread_created));
        }

    }

    if (close(listen_file_descriptor))
    {
        perror("close(listen_file_descriptor)");
    }

    sleep(DRAIN_SECONDS);

    printf("\naccepted: %lu\n", accepted);
    printf("served:   %lu\n", g_requests_served);
    printf("lost:     %ld\n", (long)accepted - (long)g_requests_served);

    return EXIT_SUCCESS;
}