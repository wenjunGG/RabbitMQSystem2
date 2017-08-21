using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Core;
using RabbitMQ.Core.Model;
using RabbitMQ.Core.Service;

namespace ConsoleApplicationRabbitMQ.Receive.Info
{
    class Program
    {
        static void Main(string[] args)
        {
            RabbitReceiveMessageService mq = new RabbitReceiveMessageService(new RabbitReceiveConfigModel()
            {
                IP = "127.0.0.1",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                VirtualHost = "loghostsfanout",
                DurableQueue = true,
                QueueName = "loghosts_system_info_fanout",
                Exchange = "ex1_fanout",
                ExchangeType = ExchangeTypeEnum.fanout,
                DurableMessage = true,
                RoutingKey = "system.info_fanout"
            });

            Program p = new Program();
            mq.Receive<string>(p.GetMessage);
            Console.ReadLine();
        }

        public void GetMessage(string value)
        {
            try
            {
                Console.WriteLine("接受到数据：" + value);
            }
            catch (Exception ex)
            {

            }
        }
    }
}
