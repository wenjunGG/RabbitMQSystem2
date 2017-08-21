using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Core.Model
{
    /// <summary>
    /// 接受消息以后针对消息的最终处理结果，用于通知RabbitMQ的处理状态
    /// </summary>
    public enum ProcessingResultsEnum
    {
        /// <summary>
        /// 处理成功
        /// </summary>
        Accept,

        /// <summary>
        /// 可以重试的错误
        /// </summary>
        Retry,

        /// <summary>
        /// 无需重试的错误
        /// </summary>
        Reject,  
    }
}
