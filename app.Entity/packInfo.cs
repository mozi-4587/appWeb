using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.IO;

namespace app.Entity
{
    public class packInfo
    {
        private string m_urlpack;//
        private string m_datpack;//
        private string m_dowpack;//
        private string m_userpack;//
       
        protected HttpContext httpContext;

        public packInfo(HttpContext httpContext) 
        {
            parameters = new Hashtable();

            this.httpContext = httpContext;
        }

        /// <summary>
        /// 请求的参数
        /// </summary>
        protected Hashtable parameters;
        public void setReqParameters(string[] paramNames)
        {
            //string[] Items = this.httpContext.Request.QueryString.AllKeys;
          
            this.parameters.Clear();
            foreach (string pName in paramNames)
            {
                if (!string.IsNullOrEmpty(pName))
                {
                    string reqVal = this.httpContext.Request[pName];
                    if (String.IsNullOrEmpty(reqVal))
                    {
                        continue;
                    }
                    this.parameters.Add(pName, reqVal);
                }
            }
        }


        public void setReqParameters()
        {
            string[] paramNames = this.httpContext.Request.QueryString.AllKeys;

            this.parameters.Clear();
            foreach (string pName in paramNames)
            {
                string reqVal = this.httpContext.Request[pName];
                if (String.IsNullOrEmpty(reqVal))
                    continue;
              
                this.parameters.Add(pName, reqVal);
            }
        }


        public void setReqParametersInfo()
        {
            dataPack=Pack();
            string[] paramNames = this.httpContext.Request.QueryString.AllKeys;
            if (paramNames.Length == 0)
                paramNames = this.httpContext.Request.Form.AllKeys;

            this.parameters.Clear();
            foreach (string pName in paramNames)
            {
                string reqVal = this.httpContext.Request[pName];
                if (String.IsNullOrEmpty(reqVal))
                    continue;

                this.parameters.Add(pName, reqVal);
            }
        }

        public void setReqParametersInfo(string code)
        {
            dataPack = Pack();
            if (code == "UrlDecode")
                dataPack = System.Web.HttpUtility.UrlDecode(dataPack);
            string[] paramNames = this.httpContext.Request.QueryString.AllKeys;
            if (paramNames.Length == 0)
                paramNames = this.httpContext.Request.Form.AllKeys;

            this.parameters.Clear();
            foreach (string pName in paramNames)
            {
                string reqVal = this.httpContext.Request[pName];
              
                if (String.IsNullOrEmpty(reqVal))
                    continue;
                if (code == "UrlDecode")
                    this.parameters.Add(pName, System.Web.HttpUtility.UrlDecode(reqVal));
                else
                    this.parameters.Add(pName, System.Web.HttpUtility.UrlDecode(reqVal));
            }
        }

        /*public void setReqParameters(Hashtable parameters)
        {
          
            this.parameters.Clear();

            this.parameters = parameters;
            
        }*/

        // <summary>
        /// 获取所有参数
        /// </summary>
        /// <returns></returns>
        public Hashtable getAllParameters()
        {
            return this.parameters;
        }

       

        public string urlpack
        {
            get { return this.httpContext.Request.Url.ToString(); }
            //set { m_urlpack = this.httpContext; }
        }

      
        public string Pack()
        {
            
                using (StreamReader sr = new StreamReader(this.httpContext.Request.InputStream))
                {
                    return  sr.ReadToEnd();
                }
               
        }

        public string dataPack
        {
            get { return m_datpack; }
            set { m_datpack = value; }
              
        }

        public string userPack
        {
            get { return m_userpack; }
            set { m_userpack = value; }

        }


        public string dowPack
        {
            get { return m_dowpack; }
            set { m_dowpack = value; }

        }

        public string getValue(string key)
        {
            foreach (DictionaryEntry de in this.parameters)
            {
                string name = de.Key.ToString().ToLower();
                if (key == name)
                    return de.Value.ToString();
            }

            return null;
        }

        

    }
}
