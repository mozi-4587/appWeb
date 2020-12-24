using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using log4net;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]
namespace app.Common
{
     
       public class LogHelper
       {
          //private static readonly log4net.ILog log = log4net.LogManager.GetLogger("WebLogger");
           /// <summary>

           /// 输出日志到Log4Net

           /// </summary>

           /// <param name="t"></param>

           /// <param name="ex"></param>

           #region static void WriteLog(Type t, Exception ex)

           public static void WriteLog(Type t, Exception ex)
           {

               log4net.ILog log = log4net.LogManager.GetLogger(t);

               log.Error("Error", ex);

           }

           #endregion

           /// <summary>

           /// 输出日志到Log4Net

           /// </summary>

           /// <param name="t"></param>

           /// <param name="msg"></param>

           #region static void WriteLog(Type t, string msg)

           public static void WriteLog(Type t, string msg)
           {

               log4net.ILog log = log4net.LogManager.GetLogger(t);

               log.Error(msg);

           }

           #endregion

      
    }
}
