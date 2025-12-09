using SistemaDeAnalisis;
using SistemaDeAnalisis.Configuration;
using SistemaDeAnalisis.Extractors;
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Services;
using SistemaDeAnalisis.Services.Loaders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            Console.WriteLine(" INICIANDO DIAGNÓSTICO DEL SISTEMA ETL...");

            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    Console.WriteLine(" Configurando servicios...");

                    services.Configure<ETLConfiguration>(context.Configuration.GetSection("ETL"));

                    services.AddHttpClient<ApiExtractor>();

                    // === LOADERS BASE ===
                    services.AddSingleton<DataLoader>();

                    // === LOADERS DE DIMENSIONES ===
                    services.AddSingleton<DimCustomerLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        return new DimCustomerLoader(cfg.Value.ConnectionString);
                    });

                    services.AddSingleton<DimProductLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        return new DimProductLoader(cfg.Value.ConnectionString);
                    });

                    services.AddSingleton<DimOrderLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        return new DimOrderLoader(cfg.Value.ConnectionString);
                    });

                    // === LOADER DE FACTS === ?
                    services.AddSingleton<FactSalesLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        var logger = sp.GetRequiredService<ILogger<FactSalesLoader>>();
                        return new FactSalesLoader(logger, cfg);
                    });

                    services.AddSingleton<ExtractionService>();

                    services.AddSingleton<IExtractor, CsvExtractor>();
                    services.AddSingleton<IExtractor, DatabaseExtractor>();
                    services.AddSingleton<IExtractor, ApiExtractor>();

                    services.AddHostedService<Worker>();
                })
                .ConfigureLogging((context, logging) =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.AddDebug();
                    logging.AddConfiguration(context.Configuration.GetSection("Logging"));
                })
                .Build();

            Console.WriteLine(" Iniciando aplicación principal...");
            Console.WriteLine("==========================================");

            await host.RunAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($" ERROR CRÍTICO EN LA APLICACIÓN: {ex}");
            Console.WriteLine("Presiona Enter para salir...");
            Console.ReadLine();
        }
    }
}

