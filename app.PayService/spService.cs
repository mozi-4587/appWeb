using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

using app.Common;
using app.Entity;
using app.Data;
using app.Service;

namespace app.PayService
{
    public class SpService
    {
        /// <summary>
        /// 插入请求数据
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static int payReception(string info)
        {
            int olid = 0;

            string filed = "";
            string value = "";
            try
            {

                JObject jsonObj = JObject.Parse(info);
                IEnumerable<JProperty> properties = jsonObj.Properties();

                foreach (JProperty item in properties)
                {
                    if (!string.IsNullOrEmpty(item.Value.ToString()))
                    {
                        filed+= "," + item.Name.ToString();

                        value+= ",'" + item.Value.ToString() + "'";
                    }
                }

                string sql = "INSERT INTO [spdataInfo_2016](" + filed.Substring(1) + ") VALUES(" + value.Substring(1) + ")";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(SpService), info + "============>payReception数据查入失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), info + "============>payReception数据插入异常[" + e.ToString() + "]");
                return olid;
            }
            return olid;
        }


        /// <summary>
        /// 插入同步数据
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static int insertSyncData(string info)
        {
            int olid = 0;
            try
            {
               /*JObject jsonObj = JObject.Parse(info);

                string sql = "INSERT INTO [tab_sync]([OrderNo],[phoneNo],[phoneSerial],[phoneType],[orderFee],[productNo],[serviceTel],[Name],[Num],[orderStatus],[companyID],[ordertype],[response],[sucTime],[urlparam],[Area],[datatime]) "
                + "VALUES('" + jsonObj["OrderNo"].ToString() + "','" + jsonObj["phoneNo"].ToString() + "','" + jsonObj["phoneSerial"].ToString() + "','" + jsonObj["phoneType"].ToString() + "'," + jsonObj["orderFee"].ToString() + ",'"
                + jsonObj["productNo"].ToString() + "','" + jsonObj["serviceTel"].ToString() + "','" + jsonObj["Name"].ToString() + "','" + jsonObj["Num"].ToString() + "'," + jsonObj["orderStatus"].ToString() + ","
                + jsonObj["companyID"].ToString() + "," + jsonObj["ordertype"].ToString() + ",'" + jsonObj["response"].ToString() + "','" + jsonObj["sucTime"].ToString() + "','" + jsonObj["urlparam"].ToString() + "','" + jsonObj["Area"].ToString() + "');SELECT SCOPE_IDENTITY()";*/
                //olid = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).ToString(), 0);

                string sql = "INSERT INTO [tab_sync]([OrderNo],[phoneNo],[phoneSerial],[phoneType],[orderFee],[productNo],[serviceTel],[Name],[Num],[orderStatus],[companyID],[ordertype],[response],[sucTime],[urlparam],[Area],[datatime])"
                    + " select [OrderNo],[phoneNo],[phoneSerial],[phoneType],[orderFee],[productNo],[serviceTel],[Name],[Num],[orderStatus],[companyID],[ordertype],[response],[sucTime],[urlparam],[Area],[datatime] from spdataInfo_2016 where OrderNo='"+info+"'";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(SpService), info + "============>insertSyncData数据查入失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), info + "============>insertSyncData数据插入异常[" + e.ToString() + "]");
                return olid;
            }
            return olid;
        }



        /// <summary>
        /// 获取用户信息
        /// </summary>
        /// <param name="code"></param>
        /// <returns></returns>
        public static string getCompanyInfo(string code)
        {
            string info = null;
            string sql = "SELECT top 1 a.* FROM company as a where a.code =(SELECT max(code) FROM company where code=left('"+code+"',LEN(code)))";
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter());
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), sql + "============>getCompanyInfo获取数据异常[" + e.ToString() + "]");

            }

            return info.Replace("[","").Replace("]","");

        }


        public static string getCompanyInfo(int code)
        {
            string info = null;
            string sql = "SELECT top 1 * FROM company where infoid =" + code;
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter());
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), sql + "============>getCompanyInfo获取数据异常[" + e.ToString() + "]");

            }
            return info.Replace("[", "").Replace("]", "");
        }

        /// <summary>
        /// 用户帐号验证
        /// </summary>
        /// <param name="coopOrderNo"></param>
        /// <returns></returns>
        public static string Authentication(string coopOrderNo)
        {
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + System.Configuration.ConfigurationManager.AppSettings["format"] + "payRetunXml.xml";
            System.Xml.XmlDocument xmlDoc = new System.Xml.XmlDocument();
            xmlDoc.Load(path);
            System.Xml.XmlNode root = xmlDoc.SelectSingleNode("response");
            root.SelectSingleNode("coopOrderNo").InnerText = coopOrderNo;

            return xmlDoc.OuterXml;
        }

        public static string Authentication(string coopOrderNo, string code)
        {

            JObject jsonObj = JObject.Parse(Service.Service.getFormantZH("payRetCode", System.Text.Encoding.UTF8));
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + System.Configuration.ConfigurationManager.AppSettings["format"] + "payRetunXml.xml";
            System.Xml.XmlDocument xmlDoc = new System.Xml.XmlDocument();
            xmlDoc.Load(path);
            System.Xml.XmlNode root = xmlDoc.SelectSingleNode("response");
            root.SelectSingleNode("coopOrderNo").InnerText = coopOrderNo;
            root.SelectSingleNode("retCode").InnerText = code;
            root.SelectSingleNode("retMsg").InnerText = jsonObj[code].ToString();
            return xmlDoc.OuterXml;
        }

        public static string getSmsSuccess(string xmlcode,string code)
        {
            System.Xml.XmlDocument xmlDoc = new System.Xml.XmlDocument();
            xmlDoc.LoadXml(xmlcode);
            System.Xml.XmlNode root = xmlDoc.SelectSingleNode("response");
           
            root.SelectSingleNode("smsContent").InnerText = code;
            return xmlDoc.OuterXml.Replace("&amp;", "&");
        }

        /// <summary>
        /// 设置数据同步状态
        /// </summary>
        /// <returns></returns>
        public static void setSyncStatus(int id)
        {
            int olid = 0;
            string sql = null;
            try
            {
                sql = "update [tab_sync] set result=0 where infoid=" + id;
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), sql + "============>setSyncStatus更新数据异常[" + e.ToString() + "]");

            }
        }

        public static void setSyncStatus(string orderno)
        {
            int olid = 0;
            string sql = null;
            try
            {
                sql = "update [tab_sync] set result=0 where OrderNo='" + orderno + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), sql + "============>setSyncStatus更新数据异常[" + e.ToString() + "]");

            }
        }

        /// <summary>
        /// 获取sp产品数据
        /// </summary>
        /// <param name="fee"></param>
        /// <returns></returns>
        public static string getSPProduct(string fee)
        {
            string info = null;
            string sql = "SELECT top 1 * FROM spproduct where price ='" + fee + "'";
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter());
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), sql + "============>getSPProduct获取数据异常[" + e.ToString() + "]");

            }

            return info.Replace("[", "").Replace("]", "");
        }


        /// <summary>
        /// 获取sp产品数据
        /// </summary>
        /// <param name="fee"></param>
        /// <returns></returns>
        public static string getSPProduct(string code,string fee)
        {
            string info = null;
            string sql = "SELECT top 1 * FROM spproduct where price ='" + fee + "' and companycode='" + code + "'";
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter());
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(SpService), sql + "============>getSPProduct获取数据异常[" + e.ToString() + "]");

            }

            return info.Replace("[", "").Replace("]", "");
        }


        /// <summary>
        /// 查询订单是否存在
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static int searchVerifyData(string info)
        {
            int flag = 0;
            string sql = "select CASE WHEN (select count(infoid) as num from [spdataInfo_2016] as b where OrderNo='" + info + "')>0  then 1  ELSE 0  END";
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                flag = Convert.ToInt32(old);

            return flag;
        }



    }
}
