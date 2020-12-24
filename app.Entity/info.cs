using System;
using System.Collections.Generic;
using System.Text;

namespace app.Entity
{
    public class info
    {
        
        private string m_imsi;//
        private string m_phoneNumber;//
        private string m_transId;//
        private string m_processTime;	//
        private string m_resCode;//
        private string m_appid;	//
        private string m_cpid;	//
        private string m_cpparm;	//
        private string m_fee;	//
        private string m_fmt;//
        private string m_ib;//
        private string m_mt;//
        private string m_type;//
        private string m_imei;//
        private string m_table;//

                public info() { }

                public string Imsi
                {
                    get { return m_imsi; }
                    set { m_imsi = value; }
                }

                public string PhoneNumber
                {
                    get { return m_phoneNumber; }
                    set { m_phoneNumber = value; }
                }

                public string TransId
                {
                    get { return m_transId; }
                    set { m_transId = value; }
                }

                public string ProcessTime
                {
                    get { return m_processTime; }
                    set { m_processTime = value; }
                }

                public string ResCode
                {
                    get { return m_resCode; }
                    set { m_resCode = value; }
                }

                public string Appid
                {
                    get { return m_appid; }
                    set { m_appid = value; }
                }

                public string Cpid
                {
                    get { return m_cpid; }
                    set { m_cpid = value; }
                }
                public string Cpparm
                {
                    get { return m_cpparm; }
                    set { m_cpparm = value; }
                }
                public string Fee
                {
                    get { return m_fee; }
                    set { m_fee = value; }
                }

                public string Fmt
                {
                    get { return m_fmt; }
                    set { m_fmt = value; }
                }

                public string IB
                {
                    get { return m_ib; }
                    set { m_ib = value; }
                }

                public string _MT
                {
                    get { return m_mt; }
                    set { m_mt = value; }
                }
                public string Type
                {
                    get { return m_type; }
                    set { m_type = value; }
                }
                public string Imei
                {
                    get { return m_imei; }
                    set { m_imei = value; }
                }

                public string Table
                {
                    get {
                        DateTime dt = DateTime.Now;
                        return dt.ToString("yyyy-M");
                    }
                }
    }
}
