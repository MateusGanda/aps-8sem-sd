using System.Text.Json;
using AirQuality.Api.Data;
using AirQuality.Api.Services;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AirQuality.Api.Messaging;

/// <summary>
/// Consumidor da fila de leituras: avalia a regra de alerta fora do POST.
/// Cada réplica da API roda um consumidor na MESMA fila (consumidores concorrentes):
/// o RabbitMQ entrega cada mensagem a apenas um deles.
/// O ack é manual, depois de avaliar. Se o pod morrer no meio, a mensagem volta para a fila
/// (entrega "ao menos uma vez").
/// </summary>
public sealed class AlertConsumer(ReadingQueue queue, IServiceScopeFactory scopes, ILogger<AlertConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!queue.Enabled)
        {
            logger.LogInformation("Fila não configurada: os alertas são avaliados dentro do POST.");
            return;
        }

        // Libera a partida da API: conectar na fila não pode atrasar a abertura da porta HTTP
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var channel = queue.CreateChannel();
                // No máximo 10 mensagens sem ack por consumidor: o resto fica na fila para as outras réplicas
                channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += (_, delivery) => HandleAsync(channel, delivery, stoppingToken);
                channel.BasicConsume(ReadingQueue.QueueName, autoAck: false, consumer);
                logger.LogInformation("Consumindo a fila {Queue}.", ReadingQueue.QueueName);

                // Fica aqui até o canal cair (broker fora) ou a API encerrar
                var closed = new TaskCompletionSource();
                channel.ModelShutdown += (_, _) => closed.TrySetResult();
                await using var registration = stoppingToken.Register(() => closed.TrySetResult());
                await closed.Task;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning("Fila indisponível ({Error}). Nova tentativa em 5s.", ex.Message);
            }

            try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
            catch (OperationCanceledException) { }
        }
    }

    private async Task HandleAsync(IModel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        try
        {
            var message = JsonSerializer.Deserialize<ReadingCreated>(delivery.Body.Span)!;

            using var scope = scopes.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AirQualityDbContext>();
            var alerts = scope.ServiceProvider.GetRequiredService<AlertService>();

            var reading = await db.ParticulateReadings.AsNoTracking().FirstOrDefaultAsync(r => r.Id == message.ReadingId, ct);
            if (reading is not null)
                await alerts.EvaluateParticulateAsync(reading, ct);

            channel.BasicAck(delivery.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Falha ao avaliar a mensagem {DeliveryTag}. Devolvendo para a fila.", delivery.DeliveryTag);
            // Espera um pouco antes de devolver: se o banco estiver fora, evita um laço apertado de reentrega
            try { await Task.Delay(TimeSpan.FromSeconds(1), ct); } catch (OperationCanceledException) { }
            if (channel.IsOpen)
                channel.BasicNack(delivery.DeliveryTag, multiple: false, requeue: true);
        }
    }
}
