using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;

namespace ThermalInversion.Api.Services;

/// <summary>
/// Cache das consultas agregadas no Redis (cache-aside): procura no cache; se não achar, consulta o banco e guarda.
/// Como o cache fica FORA dos pods, todas as réplicas compartilham o mesmo resultado.
/// A resposta leva o cabeçalho X-Cache: HIT (veio do cache), MISS (veio do banco) ou BYPASS (cache fora do ar).
/// </summary>
public class QueryCache(IDistributedCache cache, IConfiguration configuration, ILogger<QueryCache> logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    // Tempo de vida curto: o dado pode ficar até este tanto desatualizado
    private readonly TimeSpan _ttl = TimeSpan.FromSeconds(configuration.GetValue("Cache:TtlSeconds", 10));

    public async Task<T?> GetOrCreateAsync<T>(HttpResponse response, string key, Func<Task<T?>> query, CancellationToken ct)
        where T : class
    {
        try
        {
            var cached = await cache.GetAsync(key, ct);
            if (cached is not null)
            {
                response.Headers["X-Cache"] = "HIT";
                return JsonSerializer.Deserialize<T>(cached, Json);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Cache fora do ar não derruba a consulta: vai direto ao banco
            logger.LogWarning("Cache indisponível ({Error}). Consultando o banco.", ex.Message);
            response.Headers["X-Cache"] = "BYPASS";
            return await query();
        }

        var value = await query();
        response.Headers["X-Cache"] = "MISS";
        if (value is null)
            return null;

        try
        {
            await cache.SetAsync(key, JsonSerializer.SerializeToUtf8Bytes(value, Json),
                new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = _ttl }, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Não foi possível gravar no cache ({Error}).", ex.Message);
        }
        return value;
    }
}
