using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace app.Entity
{
    public class jsoninfo
    {
        private int m_code;//
        private string m_initPort;//
        private string m_initSms;//
        private string m_message;	//
        private string m_registerPort;//
        private string m_registerSms;	//
        private string m_transId;	//

        public jsoninfo() { }

        public int Code
        {
            get { return m_code; }
            set { m_code = value; }
        }

        public string InitPort
        {
            get { return m_initPort; }
            set { m_initPort = value; }
        }

        public string InitSms
        {
            get { return m_initSms; }
            set { m_initSms = value; }
        }

        public string Message
        {
            get { return m_message; }
            set { m_message = value; }
        }

        public string RegisterPort
        {
            get { return m_registerPort; }
            set { m_registerPort = value; }
        }

        public string RegisterSms
        {
            get { return m_registerSms; }
            set { m_registerSms = value; }
        }

        public string TransId
        {
            get { return m_transId; }
            set { m_transId = value; }
        }
       
    }
}
