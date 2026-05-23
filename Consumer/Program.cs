using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

var queue = "letterbox";
await channel.QueueDeclareAsync(
    queue: queue,
    durable: false,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
var random = new Random();

consumer.ReceivedAsync += async (model, ea) =>
{
    var processingTime = random.Next(1, 6);
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($"Received Message: {message} will take {processingTime} to processed");
    Task.Delay(TimeSpan.FromSeconds(processingTime)).Wait();
    await channel.BasicAckAsync(ea.DeliveryTag, false);
};

await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);

Console.ReadKey();