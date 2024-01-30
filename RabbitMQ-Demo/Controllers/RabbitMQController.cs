using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQ_Demo.Controllers
{
    [Route("api/[controller]")]
    [ApiController]

    public class RabbitMQController : ControllerBase
    {
        private string RabbitMqConfigration = string.Empty;
        private  IConnection _rabbitMqConnection;
        private IModel _emailChannel;
        private IModel _smsChannel;
        public string message = string.Empty;
        public RabbitMQController()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            RabbitMqConfigration = config.GetSection("ConnectionString").GetValue<string>("RabbitMqConnectionString")!;
            var connectionFactory = new ConnectionFactory();
            connectionFactory.Uri = new Uri(RabbitMqConfigration);
            connectionFactory.AutomaticRecoveryEnabled = true;
            connectionFactory.DispatchConsumersAsync = true;
            _rabbitMqConnection = connectionFactory.CreateConnection("DemoAppClient");
        }
       
        [HttpGet("CreateExchange")]
        public async Task<IActionResult> CreateExchange()
        {
            using (var channel = _rabbitMqConnection.CreateModel())
            {
                channel.ExchangeDeclare("CustomerNotification", ExchangeType.Direct, true, false);
            }
            return Ok();
        }

        [HttpGet("CreateQueues")]
        public async Task<IActionResult> CreateQueue()
        {
            using (var channel = _rabbitMqConnection.CreateModel())
            {
                channel.QueueDeclare("Email",true,false,false);
                channel.QueueDeclare("Sms",true,false,false);
            }
            return Ok();
        }
        [HttpGet("BindingQueue")]
        public async Task<IActionResult> BindQueue()
        {
            using (var channel = _rabbitMqConnection.CreateModel())
            {
                channel.QueueBind("Email", "CustomerNotification", "email");
                channel.QueueBind("Sms", "CustomerNotification", "sms");
            }
                return Ok();
        }

        [HttpPost("PublishEmailMessage")]
        public async Task<IActionResult> PublishEmailMessage(Message message)
        {
            using (var channel = _rabbitMqConnection.CreateModel())
            {
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;
                channel.BasicPublish("CustomerNotification","email",properties,Encoding.UTF8.GetBytes(message.message));
            }
            return Ok();
        }   
        [HttpPost("PublishSmsMessage")]
        public async Task<IActionResult> PublishSmsMessage(Message message)
        {
            using (var channel = _rabbitMqConnection.CreateModel())
            {
                var properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;
                channel.BasicPublish("CustomerNotification","sms",properties,Encoding.UTF8.GetBytes(message.message));
            }
            return Ok();
        }

        [HttpGet("ConsumeEmailMessage")]
        public async Task<IActionResult> ConsumeEmailMessage()
        {
            _emailChannel = _rabbitMqConnection.CreateModel();
            _emailChannel.BasicQos(0,1,false);
            var emailChannelConsumer = new AsyncEventingBasicConsumer(_emailChannel);
            emailChannelConsumer.Received += EmailChannelConsumer_Recevied;
            _emailChannel.BasicConsume("Email", false, emailChannelConsumer);
            return Ok("thx");
        }

        private async Task EmailChannelConsumer_Recevied(object sender, BasicDeliverEventArgs e)
        {
            message = Encoding.UTF8.GetString(e.Body.ToArray());
            _emailChannel.BasicAck(e.DeliveryTag, false);
        }

        [HttpGet("ConsumeSmsMessage")]
        public async Task<IActionResult> ConsumeSmsMessage()
        {
            _emailChannel = _rabbitMqConnection.CreateModel();
            _emailChannel.BasicQos(0,1,false);
            var smsChannelConsumer = new AsyncEventingBasicConsumer(_emailChannel);
            smsChannelConsumer.Received += SmsChannelConsumer_Recevied;
            _emailChannel.BasicConsume("Sms", false, smsChannelConsumer);
            return Ok("thx");
        }

        private async Task SmsChannelConsumer_Recevied(object sender, BasicDeliverEventArgs e)
        {
            message = Encoding.UTF8.GetString(e.Body.ToArray());
            _emailChannel.BasicAck(e.DeliveryTag, false);
        }
    }
}
