using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Core.Model
{
    /// <summary>
    /// RabbitMQ 发送方的配置
    /// </summary>
    public class RabbitReceiveConfigModel
    {
        /// <summary>
        /// 服务器IP地址
        /// </summary>
        public string IP { get; set; }

        /// <summary>
        /// 服务器端口，默认是 5672
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 路由名称
        /// </summary>
        public string Exchange { get; set; }

        /// <summary>
        /// 路由的类型枚举
        /// </summary>
        public ExchangeTypeEnum ExchangeType { get; set; }

        /// <summary>
        /// 路由的关键子
        /// </summary>
        public string RoutingKey { get; set; }

        /// <summary>
        /// 登录用户名
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 登录密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// 虚拟主机名称
        /// </summary>
        public string VirtualHost { get; set; }

        /// <summary>
        /// 队列名称
        /// </summary>
        public string QueueName { get; set; }

        /// <summary>
        /// 是否持久化该队列
        /// </summary>
        public bool DurableQueue { get; set; }

        /// <summary>
        /// 是否持久化队列中的消息
        /// </summary>
        public bool DurableMessage { get; set; }

    }
}
