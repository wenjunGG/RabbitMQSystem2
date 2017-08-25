using RabbitMQ.Core;
using RabbitMQ.Core.Model;
using RabbitMQ.Core.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplicationRabbitMQ.Send
{
    class Program
    {
        static void Main(string[] args)
        {
            Test2();
        }

        /// <summary>
        /// 测试2
        /// </summary>
        public static void Test2()
        {
            RabbitSendMessageService mq = new RabbitSendMessageService(new RabbitSendConfigModel()
            {
                IP = "127.0.0.1",
                UserName = "guest",
                Password = "guest",
                Port = 15672,
                VirtualHost = "loghostsfanout",
                DurableQueue = true,
                QueueName = "",
                Exchange = "ex1_Topic",
                ExchangeType = ExchangeTypeEnum.topic,
                DurableMessage = true,
                RoutingKey = "bug" //bug error info
            });

            for (int i = 0; i < 10000000; i++)
            {
                int index = new Random().Next(1, 3);
                switch (index)
                {
                    case 1:
                        mq.RabbitConfig.RoutingKey = "system.error_Topic";
                        break;
                    case 2:
                        mq.RabbitConfig.RoutingKey = "system.info_Topic";
                        break;
                    case 3:
                        mq.RabbitConfig.RoutingKey = "system.bug_Topic";
                        break;
                }

                string value = i.ToString() + "  Exchange=" + mq.RabbitConfig.Exchange + "   ExchangeType=" + mq.RabbitConfig.ExchangeType.ToString() + "     RoutingKey=" + mq.RabbitConfig.RoutingKey;
                mq.Send<string>(value);

                Console.WriteLine("消息已发送：" + value);
                Thread.Sleep(500);
            }
        }
    }
}
