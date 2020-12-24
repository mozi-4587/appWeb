using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Mail;
using System.Net;

namespace app.Common
{
    public class Email
    {
        /// <summary>
        /// 发件人
        /// </summary>
        public static string mailFrom { get; set; }

        /// <summary>
        /// 收件人
        /// </summary>
        public static string[] mailToArray { get; set; }

        /// <summary>
        /// 抄送
        /// </summary>
        public static string[] mailCcArray { get; set; }

        /// <summary>
        /// 标题
        /// </summary>
        public static string mailSubject { get; set; }

        /// <summary>
        /// 正文
        /// </summary>
        public static string mailBody { get; set; }

        /// <summary>
        /// 发件人密码
        /// </summary>
        public static string mailPwd { get; set; }

        /// <summary>
        /// SMTP邮件服务器
        /// </summary>
        public static string host { get; set; }

        /// <summary>
        /// 邮件服务器端口
        /// </summary>
        public static int port { get; set; }

        /// <summary>
        /// 正文是否是html格式
        /// </summary>
        public static bool isbodyHtml { get; set; }

        /// <summary>
        /// 附件
        /// </summary>
        public static string[] attachmentsPath { get; set; }

        public static bool Send()
        {
            //使用指定的邮件地址初始化MailAddress实例
            MailAddress maddr = new MailAddress(mailFrom);

            //初始化MailMessage实例
            MailMessage myMail = new MailMessage();

            //向收件人地址集合添加邮件地址
            if (mailToArray != null)
            {
                for (int i = 0; i < mailToArray.Length; i++)
                {
                    myMail.To.Add(mailToArray[i].ToString());
                }
            }

            //向抄送收件人地址集合添加邮件地址
            if (mailCcArray != null)
            {
                for (int i = 0; i < mailCcArray.Length; i++)
                {
                    myMail.CC.Add(mailCcArray[i].ToString());
                }
            }
            //发件人地址
            myMail.From = maddr;

            //电子邮件的标题
            myMail.Subject = mailSubject;

            //电子邮件的主题内容使用的编码
            myMail.SubjectEncoding = Encoding.UTF8;

            //电子邮件正文
            myMail.Body = mailBody;

            //电子邮件正文的编码
            myMail.BodyEncoding = Encoding.Default;

            //电子邮件优先级
            myMail.Priority = MailPriority.High;

            //电子邮件不是html格式
            myMail.IsBodyHtml = isbodyHtml;

            //在有附件的情况下添加附件
            try
            {
                if (attachmentsPath != null && attachmentsPath.Length > 0)
                {
                    Attachment attachFile = null;
                    foreach (string path in attachmentsPath)
                    {
                        attachFile = new Attachment(path);
                        myMail.Attachments.Add(attachFile);
                    }
                }
            }
            catch (Exception err)
            {
                throw new Exception("在添加附件时有错误:" + err.Message);
            }

            SmtpClient client = new SmtpClient();

            //指定发件人的邮件地址和密码以验证发件人身份
            client.Credentials = new NetworkCredential(mailFrom, mailPwd);

            //设置SMTP邮件服务器
            //client.Host = "smtp." + myMail.From.Host;
            client.Host = host;

            //SMTP邮件服务器端口
            client.Port = port;

            //是否使用安全连接
            //client.EnableSsl = true;

            try
            {
                //将邮件发送到SMTP邮件服务器
                client.Send(myMail);
                return true;
            }
            catch (SmtpException ex)
            {
                string msg = ex.Message;
                return false;
            }
        }
    }
}
