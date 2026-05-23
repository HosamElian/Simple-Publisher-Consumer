
using System.Text;
using RabbitMQ.Client;

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

var messageId = 1;
var random = new Random();

while (true)
{
    var publishingTime = random.Next(1, 4);
    var message = $"Sending MessageId: {messageId}";

    var encodedMessage = Encoding.UTF8.GetBytes(message);

    await channel.BasicPublishAsync("", queue, encodedMessage);

    System.Console.WriteLine($"Published Message: {message}");

    Task.Delay(TimeSpan.FromSeconds(publishingTime)).Wait();
    messageId++;
}

