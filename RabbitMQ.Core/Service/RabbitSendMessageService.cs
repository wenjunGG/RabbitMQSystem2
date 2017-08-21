using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Core.Service
{
    /// <summary>
    /// 发送消息的服务 
    /// </summary>
    public class RabbitSendMessageService
    {
        /// <summary>
        /// 服务器配置
        /// </summary>
        public RabbitSendConfigModel RabbitConfig { get; set; }

        public static IConnection Connection;

        public RabbitSendMessageService(RabbitSendConfigModel config)
        {
            this.RabbitConfig = config;
        }

        /// <summary>
        /// 获取队列服务器的链接
        /// </summary>
        /// <returns></returns>
        public IConnection GetConnection()
        {
            if (Connection == null)
                Connection = RabbitBaseService.GetConnection(this.RabbitConfig.IP, this.RabbitConfig.Port, this.RabbitConfig.UserName, this.RabbitConfig.Password, this.RabbitConfig.VirtualHost, 60);

            return Connection;
        }

        /// <summary>
        /// 发送消息，泛型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message"></param>
        /// <returns></returns>
        public bool Send<T>(T message)
        {
            string value = JsonConvert.SerializeObject(message);
            return this.Send(value);
        }

        /// <summary>
        /// 发送消息给队列服务器
        /// </summary>
        /// <returns></returns>
        public bool Send(string message)
        {
            if (string.IsNullOrWhiteSpace(message)) return false;

            try
            {
                using (var channel = this.GetConnection().CreateModel())
                {
                    if (!string.IsNullOrWhiteSpace(this.RabbitConfig.Exchange))
                        //使用自定义的路由
                        channel.ExchangeDeclare(this.RabbitConfig.Exchange, this.RabbitConfig.ExchangeType.ToString(), this.RabbitConfig.DurableQueue, false, null);
                    else
                        //声明消息队列，且为可持久化的  ，如果队列的名称不存在，系统会自动创建，有的话不会覆盖
                        channel.QueueDeclare(this.RabbitConfig.QueueName, this.RabbitConfig.DurableQueue, false, false, null);

                    IBasicProperties properties = channel.CreateBasicProperties();
                    properties.DeliveryMode = Convert.ToByte(this.RabbitConfig.DurableMessage ? 2 : 1); //支持持久化数据

                    //推送消息
                    byte[] bytes = Encoding.UTF8.GetBytes(message);

                    //将详细写入队列
                    if (string.IsNullOrEmpty(this.RabbitConfig.Exchange))
                        //没有配置路由，使用系统默认的路由
                        //推送消息
                        channel.BasicPublish("", this.RabbitConfig.QueueName, properties, bytes);
                    else
                        //推送消息
                        channel.BasicPublish(this.RabbitConfig.Exchange, this.RabbitConfig.RoutingKey, properties, bytes);

                    return true;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

    }
}
