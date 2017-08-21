using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Core.Service
{
    /// <summary>
    /// 声明处理接受信息的委托
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="value"></param>
    public delegate void ReceiveMessageDelegate<T>(T value);

    /// <summary>
    ///  接受消息的服务 
    /// </summary>
    public class RabbitReceiveMessageService
    {
        /// <summary>
        /// 服务器配置
        /// </summary>
        public RabbitReceiveConfigModel RabbitConfig { get; set; }

        public static IConnection Connection;

        public RabbitReceiveMessageService(RabbitReceiveConfigModel config)
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
        /// 接受消息，使用委托进行处理
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="receiveMethod"></param>
        public void Receive<T>(ReceiveMessageDelegate<T> receiveMethod)
        {
            try
            {
                using (var channel = this.GetConnection().CreateModel())
                {
                    //是否使用路由
                    if (!string.IsNullOrWhiteSpace(this.RabbitConfig.Exchange))
                    {
                        //声明路由
                        channel.ExchangeDeclare(this.RabbitConfig.Exchange, this.RabbitConfig.ExchangeType.ToString(), this.RabbitConfig.DurableQueue);

                        //声明队列且与交换机绑定
                        channel.QueueDeclare(this.RabbitConfig.QueueName, this.RabbitConfig.DurableQueue, false, false, null);
                        channel.QueueBind(this.RabbitConfig.QueueName, this.RabbitConfig.Exchange, this.RabbitConfig.RoutingKey);
                    }
                    else
                        channel.QueueDeclare(this.RabbitConfig.QueueName, this.RabbitConfig.DurableQueue, false, false, null);

                    //输入1，那如果接收一个消息，但是没有应答，则客户端不会收到下一个消息
                    channel.BasicQos(0, 1, false);
                    //在队列上定义一个消费者
                    var consumer = new QueueingBasicConsumer(channel);
                    //消费队列，并设置应答模式为程序主动应答
                    channel.BasicConsume(this.RabbitConfig.QueueName, false, consumer);

                    while (true)
                    {
                        //阻塞函数，获取队列中的消息
                        ProcessingResultsEnum processingResult = ProcessingResultsEnum.Retry;
                        ulong deliveryTag = 0;
                        try
                        {
                            Thread.Sleep(500);//暂停0.5秒，防止CPU爆满的问题

                            //获取信息
                            var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                            deliveryTag = ea.DeliveryTag;
                            byte[] bytes = ea.Body;
                            string str = Encoding.UTF8.GetString(bytes);
                            T v = JsonConvert.DeserializeObject<T>(str);
                            receiveMethod(v);

                            processingResult = ProcessingResultsEnum.Accept; //处理成功
                        }
                        catch (Exception ex)
                        {
                            processingResult = ProcessingResultsEnum.Reject; //系统无法处理的错误
                        }
                        finally
                        {
                            switch (processingResult)
                            {
                                case ProcessingResultsEnum.Accept:
                                    //回复确认处理成功
                                    channel.BasicAck(deliveryTag, false);
                                    break;
                                case ProcessingResultsEnum.Retry:
                                    //发生错误了，但是还可以重新提交给队列重新分配
                                    channel.BasicNack(deliveryTag, false, true);
                                    break;
                                case ProcessingResultsEnum.Reject:
                                    //发生严重错误，无法继续进行，这种情况应该写日志或者是发送消息通知管理员
                                    channel.BasicNack(deliveryTag, false, false);

                                    //写日志
                                    break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

        }

    }
}
