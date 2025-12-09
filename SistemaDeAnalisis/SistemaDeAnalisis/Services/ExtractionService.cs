using SistemaDeAnalisis.Extractors;
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Services;
using SistemaDeAnalisis.Services.Loaders; 
using Microsoft.Extensions.Logging;

namespace SistemaDeAnalisis.Services
{
    public class ExtractionService
    {
        private readonly ILogger<ExtractionService> _logger;
        private readonly IEnumerable<IExtractor> _extractors;

        // === LOADERS DIMENSIONALES ===
        private readonly DimCustomerLoader _customerLoader;
        private readonly DimProductLoader _productLoader;
        private readonly DimOrderLoader _orderLoader;

        // === LOADER DE FACTS ===
        private readonly FactSalesLoader _factLoader;

        private readonly DataLoader _dataLoader;

        public ExtractionService(
            ILogger<ExtractionService> logger,
            IEnumerable<IExtractor> extractors,
            DataLoader dataLoader,
            DimCustomerLoader customerLoader,
            DimProductLoader productLoader,
            DimOrderLoader orderLoader,
            FactSalesLoader factLoader)
        {
            _logger = logger;
            _extractors = extractors;

            _dataLoader = dataLoader;

            _customerLoader = customerLoader;
            _productLoader = productLoader;
            _orderLoader = orderLoader;

            _factLoader = factLoader;

            _logger.LogInformation("ExtractionService inicializado con {Count} extractors", _extractors?.Count() ?? 0);
        }

        public async Task ExecuteExtractionAsync()
        {
            _logger.LogInformation("=== INICIO DEL PROCESO ETL ===");

            var allData = new List<SalesData>();
            CsvExtractionResult? csvDims = null;

            try
            {
                if (_extractors == null || !_extractors.Any())
                {
                    _logger.LogError("NO SE ENCONTRARON EXTRACTORS REGISTRADOS");
                    return;
                }

                _logger.LogInformation("Extractors encontrados: {Count}", _extractors.Count());

                foreach (var extractor in _extractors)
                {
                    _logger.LogInformation("Procesando extractor: {Extractor}", extractor.GetType().Name);

                    if (extractor.GetType().Name == "CsvExtractor")
                    {
                        var csvExtractor = extractor as dynamic;

                        _logger.LogInformation("CsvExtractor detectado. Extrayendo DIMENSIONES…");

                        csvDims = await csvExtractor.ExtractWithDimensionsAsync();

                        allData.AddRange(csvDims.Sales);
                        continue;
                    }

                    var data = await ExtractFromSourceAsync(extractor);
                    allData.AddRange(data);
                }

                // === CARGA DE DIMENSIONES ===
                if (csvDims != null)
                {
                    _logger.LogInformation("=== CARGANDO DIMENSIONES AL DATA WAREHOUSE ===");

                    await _customerLoader.LoadAsync(csvDims.Customers);
                    await _productLoader.LoadAsync(csvDims.Products);
                    await _orderLoader.LoadAsync(csvDims.Orders);

                    _logger.LogInformation("DIMENSIONES cargadas correctamente.");
                }
                else
                {
                    _logger.LogWarning("No se encontraron dimensiones CSV para cargar.");
                }

                // === CARGA DE FACTS ===
                _logger.LogInformation("=== INICIANDO CARGA DE FACTS ===");

                await _factLoader.LoadFactSalesAsync(allData);

                _logger.LogInformation("FACTS cargados exitosamente.");

                _logger.LogInformation("=== ETL COMPLETADO ===");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ERROR DURANTE EL PROCESO ETL");
            }
        }

        private async Task<IEnumerable<SalesData>> ExtractFromSourceAsync(IExtractor extractor)
        {
            try
            {
                _logger.LogInformation("Ejecutando extractor: {ExtractorName}", extractor.GetType().Name);
                var data = await extractor.ExtractAsync();
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ERROR en extractor {ExtractorName}", extractor.GetType().Name);
                return new List<SalesData>();
            }
        }
    }
}
