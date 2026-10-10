using System.Text.Json;
using RabbitMQ.Client;

namespace AirQuality.Api.Messaging;

/// <summary>Mensagem publicada a cada leitura gravada. Leva só o id: o consumidor busca a leitura no banco.</summary>
public record ReadingCreated(long ReadingId);

/// <summary>
/// Conexão com o RabbitMQ e publicação na fila de leituras.
/// A fila é durável e as mensagens são persistentes: sobrevivem a um restart do broker.
/// Sem ConnectionStrings__RabbitMq a fila fica desligada e o alerta é avaliado dentro do POST.
/// </summary>
public sealed class ReadingQueue(IConfiguration configuration, ILogger<ReadingQueue> logger) : IDisposable
{
    public const string QueueName = "air-quality.particulate-readings";

    // Depois de uma falha de conexão, os POSTs nem tentam publicar durante este intervalo:
    // sem isso, cada requisição pagaria o timeout de conexão com o broker fora do ar.
    private static readonly TimeSpan RetryInterval = TimeSpan.FromSeconds(10);

    private readonly string? _uri = configuration.GetConnectionString("RabbitMq");
    private readonly object _lock = new();
    private IConnection? _connection;
    private IModel? _publishChannel;
    private long _retryAfterTicks;

    public bool Enabled => !string.IsNullOrEmpty(_uri);

    /// <summary>Abre um canal novo com a fila já declarada (usado pelo consumidor).</summary>
    public IModel CreateChannel()
    {
        lock (_lock)
        {
            try
            {
                if (_connection is not { IsOpen: true })
                {
                    _connection?.Dispose();
                    var factory = new ConnectionFactory
                    {
                        Uri = new Uri(_uri!),
                        DispatchConsumersAsync = true,
                        // A reconexão é feita aqui e no laço do consumidor, não pela biblioteca
                        AutomaticRecoveryEnabled = false,
                        RequestedConnectionTimeout = TimeSpan.FromSeconds(3),
                        ClientProvidedName = Environment.MachineName,
                    };
                    _connection = factory.CreateConnection();
                }

                var channel = _connection.CreateModel();
                channel.QueueDeclare(QueueName, durable: true, exclusive: false, autoDelete: false);
                return channel;
            }
            catch
            {
                Interlocked.Exchange(ref _retryAfterTicks, DateTime.UtcNow.Add(RetryInterval).Ticks);
                throw;
            }
        }
    }

    /// <summary>Publica a leitura na fila. Devolve false se a fila estiver desligada ou indisponível.</summary>
    public bool TryPublish(long readingId)
    {
        if (!Enabled || DateTime.UtcNow.Ticks < Interlocked.Read(ref _retryAfterTicks))
            return false;

        // Se outra thread estiver tentando reconectar, o POST não fica esperando: avalia o alerta ali mesmo
        if (!Monitor.TryEnter(_lock, TimeSpan.FromMilliseconds(200)))
            return false;

        try
        {
            if (_publishChannel is not { IsOpen: true })
            {
                _publishChannel?.Dispose();
                _publishChannel = CreateChannel();
            }

            var properties = _publishChannel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            _publishChannel.BasicPublish(exchange: "", routingKey: QueueName, basicProperties: properties,
                body: JsonSerializer.SerializeToUtf8Bytes(new ReadingCreated(readingId)));
            return true;
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref _retryAfterTicks, DateTime.UtcNow.Add(RetryInterval).Ticks);
            logger.LogWarning("Fila indisponível ({Error}): leitura {ReadingId} será avaliada dentro do POST.", ex.Message, readingId);
            return false;
        }
        finally
        {
            Monitor.Exit(_lock);
        }
    }

    public void Dispose()
    {
        _publishChannel?.Dispose();
        _connection?.Dispose();
    }
}
