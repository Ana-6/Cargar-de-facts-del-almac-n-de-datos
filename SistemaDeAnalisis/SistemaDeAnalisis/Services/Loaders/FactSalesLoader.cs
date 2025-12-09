using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using SistemaDeAnalisis.Configuration;
using SistemaDeAnalisis.Models;

namespace SistemaDeAnalisis.Services.Loaders
{
    public class FactSalesLoader
    {
        private readonly ILogger<FactSalesLoader> _logger;
        private readonly ETLConfiguration _config;

        public FactSalesLoader(
            ILogger<FactSalesLoader> logger,
            IOptions<ETLConfiguration> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public async Task LoadFactSalesAsync(List<SalesData> sales)
        {
            try
            {
                using var conn = new NpgsqlConnection(_config.ConnectionString);
                await conn.OpenAsync();

                _logger.LogInformation("Limpiando tabla fact_sales...");
                await conn.ExecuteAsync("SELECT cleanup_fact_sales();");

                _logger.LogInformation("Insertando registros en fact_sales...");

                string sql = @"
                    INSERT INTO fact_sales (
                        customer_key, product_key, order_key, date_key,
                        quantity, unit_price, total_price, source
                    )
                    SELECT 
                        c.customer_key,
                        p.product_key,
                        o.order_key,
                        d.date_key,
                        @Quantity,
                        @UnitPrice,
                        @TotalPrice,
                        @Source
                    FROM dim_customer c
                    JOIN dim_product p ON p.productid = @ProductID
                    JOIN dim_order o   ON o.orderid = @OrderID
                    JOIN dim_date d    ON d.full_date = @OrderDate
                    WHERE c.customerid = @CustomerID
                    LIMIT 1;
                ";

                foreach (var s in sales)
                {
                    await conn.ExecuteAsync(sql, new
                    {
                        s.CustomerID,
                        s.ProductID,
                        s.OrderID,
                        s.OrderDate,
                        s.Quantity,
                        UnitPrice = s.Price,
                        s.TotalPrice,
                        s.Source
                    });
                }

                _logger.LogInformation(" FACTS cargados correctamente: {Count}", sales.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, " Error cargando FACTS");
            }
        }
    }
}
