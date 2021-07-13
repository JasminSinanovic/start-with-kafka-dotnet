namespace Kafka.Producer
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;

    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, collection) =>
                {
                    collection.AddHostedService<KafkaProducerHostedService>();
                });
    }

    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private readonly IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;

            // Create the producer configuration
            _producer = new ProducerBuilder<Null, string>(new ProducerConfig()
            {
                // The Kafka endpoint address
                BootstrapServers = "localhost:9092",
            }).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Random random = new Random();
            while (true)
            {
                try
                {
                    int minOrders = 1;
                    int maxOrders = 205;
                    List<string> stores = CreateRandomStore();

                    Thread.Sleep(1500);

                    double averageNumber = minOrders + random.Next(50) * (maxOrders - minOrders);
                    var serrializedMessage = "{ 'data': " + stores[random.Next(stores.Count)] + ", 'orders': " + averageNumber + " }";

                    await _producer.ProduceAsync("demo-topic", new Message<Null, string>()
                    {
                        Value = serrializedMessage
                    }, cancellationToken);

                    _logger.LogInformation($"SENT: " + serrializedMessage);
                }
                catch (Exception ex)
                {
                    _logger.LogInformation("Failed" + ex.Message);
                }
            }
        }

        private List<string> CreateRandomStore()
        {
            return new List<string> { "Store-A", "Store-B", "Store-C", "Store-D", "Store-E", "Store-F" };
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}