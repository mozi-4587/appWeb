using System;
using System.Collections.Generic;
using System.Text;
using System.Web;
using System.Text.RegularExpressions;

namespace app.UrlReWriter
{
    class UrlWriter : IHttpModule
    {
        public void Init(HttpApplication context)
        {
            context.BeginRequest += new EventHandler(context_BeginRequest);
        }

        protected void context_BeginRequest(object sender, EventArgs e)
        {
            HttpApplication application = sender as HttpApplication;
            HttpContext context = application.Context; //上下文
            string url = context.Request.Url.ToString(); //获得请求URL

            Regex articleRegex = new Regex("/print/[A-Z0-9a-z_]+"); //定义规则
            if (articleRegex.IsMatch(url))
            {
                string paramStr = url.Substring(url.LastIndexOf('/') + 1);
                context.RewritePath("/appWebSite/print.aspx?id=" + paramStr);
            }
            else
            {
                //context.Response.Redirect(url);
                context.RewritePath(context.Request.Url.AbsolutePath);
            }
        }

        public void Dispose() { }
    }
}
