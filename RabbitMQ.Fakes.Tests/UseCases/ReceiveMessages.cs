using System;
using System.Collections.Generic;
using System.Text;
using FluentAssertions;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing;

namespace RabbitMQ.Fakes.Tests.UseCases
{
    [TestFixture]
    public class ReceiveMessages
    {
        private static readonly Dictionary<string, string> HeaderTemplate;
        [Test]
        public void ReceiveMessagesOnQueue()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
            SendMessage(rabbitServer,"my_exchange","hello_world");

            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // First message
                var message = channel.BasicGet("my_queue", autoAck: false);
                
                Assert.That(message,Is.Not.Null);
                var messageBody = Encoding.ASCII.GetString(message.Body);

                Assert.That(messageBody,Is.EqualTo("hello_world"));

                channel.BasicAck(message.DeliveryTag,multiple:false);
            }

        }

        [Test]
        public void ReceiveMessagesOnQueueWithBasicProperties()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
            var basicProperties = new BasicProperties
            {
                Headers = new Dictionary<string, object>() {{"TestKey", "TestValue"}},
                CorrelationId = Guid.NewGuid().ToString(),
                ReplyTo = "TestQueue",
                Timestamp = new AmqpTimestamp(123456),
                ReplyToAddress = new PublicationAddress("exchangeType", "excahngeName", "routingKey"),
                ClusterId = "1",
                ContentEncoding = "encoding",
                ContentType = "type",
                DeliveryMode = 1,
                Expiration = "none",
                MessageId = "id",
                Priority = 1,
                Type = "type",
                UserId = "1",
                AppId = "1"
            };



            SendMessage(rabbitServer, "my_exchange", "hello_world", basicProperties);
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {


                // First message
                var message = channel.BasicGet("my_queue", autoAck: false);

                Assert.That(message, Is.Not.Null);
                var messageBody = Encoding.ASCII.GetString(message.Body);

                Assert.That(messageBody, Is.EqualTo("hello_world"));

                var actualBasicProperties = message.BasicProperties;

                actualBasicProperties.Should().BeEquivalentTo(basicProperties);

                channel.BasicAck(message.DeliveryTag, multiple: false);
            }

        }

        [Test]
        public void QueueingConsumer_MessagesOnQueueBeforeConsumerIsCreated_ReceiveMessagesOnQueue()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
            SendMessage(rabbitServer, "my_exchange", "hello_world");

            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("my_queue", false, consumer);

                BasicDeliverEventArgs messageOut;
                if (consumer.Queue.Dequeue(5000, out messageOut))
                {
                    var message = (BasicDeliverEventArgs) messageOut;
                    var messageBody = Encoding.ASCII.GetString(message.Body);

                    Assert.That(messageBody, Is.EqualTo("hello_world"));

                    channel.BasicAck(message.DeliveryTag, multiple: false);
                }

                Assert.That(messageOut, Is.Not.Null);
            }

        }

        [Test]
        public void QueueingConsumer_MessagesSentAfterConsumerIsCreated_ReceiveMessagesOnQueue()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
           
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("my_queue", false, consumer);

                SendMessage(rabbitServer, "my_exchange", "hello_world");

                BasicDeliverEventArgs messageOut;
                if (consumer.Queue.Dequeue(5000, out messageOut))
                {
                    var message = (BasicDeliverEventArgs)messageOut;
                    var messageBody = Encoding.ASCII.GetString(message.Body);

                    Assert.That(messageBody, Is.EqualTo("hello_world"));

                    channel.BasicAck(message.DeliveryTag, multiple: false);
                }

                Assert.That(messageOut, Is.Not.Null);
            }

        }

        private static void SendMessage(RabbitServer rabbitServer, string exchange, string message, IBasicProperties basicProperties = null)
        {
            var connectionFactory = new FakeConnectionFactory(rabbitServer);

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: exchange, routingKey: null, mandatory: false, basicProperties: basicProperties, body: messageBody);
            }
        }

        private void ConfigureQueueBinding(RabbitServer rabbitServer, string exchangeName, string queueName)
        {
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct);

                channel.QueueBind(queueName, exchangeName, null);
            }
        }
    }
}