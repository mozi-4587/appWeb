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

namespace app.Channel
{
   public class channelService
    {
        /// <summary>
        /// 插入请求数据
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static int channelReception(string info)
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
                        filed += "," + item.Name.ToString();

                        value += ",'" + item.Value.ToString() + "'";
                    }
                }

                string sql = "INSERT INTO [channel_2016](" + filed.Substring(1) + ") VALUES(" + value.Substring(1) + ")";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(channelService), info + "============>channelReception数据查入失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), info + "============>channelReception数据插入异常[" + e.ToString() + "]");
                return olid;
            }
            return olid;
        }


       /// <summary>
       /// 更新请求数据
       /// </summary>
       /// <param name="data"></param>
       /// <returns></returns>
        public static int updateStatus(string data)
        {
            int olid = 0;
            try
            {
                JObject info = JObject.Parse(data);
                string sql = "UPDATE channel_2016 SET stat='" + info["stat"].ToString() + "',resultCode='" + info["resultCode"].ToString() + "',status='" + info["status"].ToString() + "',notifypack='" + info["notifypack"] + "' WHERE link_id='" + info["link_id"].ToString() + "'";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(channelService), "============>updateStatus数据更新失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), "updateStatus============>数据更新异常[" + e.ToString() + "]");
            }
            return olid;
        }


        // <summary>
        /// 获取下游同步信息
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        static string TransferData(string infoid)
        {
            string info = null;
            try
            {
                string sql = "select d.address as senderAddress,c.operators as MobileType,c.status,c.productCode,REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(varchar,a.datatime, 121),'-',''),':',''),'.',''),' ','') as StreamingNo,a.productCode,a.infoid,a.Mobile,a.Fee,a.msg as Message,case when a.orderresult='定购' then 0 else 1 end  as OPType,a.resultCode,a.companyID,a.Area,c.syncUrl,c.syncMethod,c.syncFlag,c.point,c.ordered from channel_2016 as a "
                       + "left join action as c on c.ordered=(SELECT max(ordered) FROM action where ordered=left(a.msg,LEN(ordered)))"
                       + " right join product as d on d.infoid=c.productID"
                       +" where a.link_id='" + infoid + "'";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).ToString().Replace("[", "").Replace("]","");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), "============>TransferData获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }


        /// <summary>
        /// 更新同步数据状态
        /// </summary>
        /// <param name="StreamingNo"></param>
        static void updateSyncStatus(JObject data)
        {
            int olid = 0;
            try
            {
                string sql = "update channel_notify set result='" + data["resultcode"].ToString() + "' where StreamingNo='" + data["streamingno"].ToString() + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(channelService), "============>updateSyncStatus更新数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), "============>updateSyncStatus更新数据异常[" + e.ToString() + "]");
            }

        }


        //向通知表添加数据
        public static int AddNotifyData(JObject code)
        {
            int olid = 0;
            try
            {
                string sql = "insert into channel_notify(infoid,StreamingNo,productCode,Mobile,Fee,Message,OPType,resultCode,companyID,Area) "
                    + "values(" + code["infoid"].ToString() + ",'" + code["StreamingNo"].ToString() + "','" + code["productCode"].ToString() + "','" + code["Mobile"].ToString() + "','" + code["Fee"].ToString() + "','" + code["Message"].ToString() + "','" + code["OPType"].ToString() + "','" + code["resultCode"].ToString() + "'," + code["companyID"].ToString() + ",'" + code["Area"].ToString() + "')";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(channelService), sql + "============>AddNotifyData添加数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), "============>AddNotifyDataa添加数据异常[" + e.ToString() + "]");
            }
            return olid;
        }

        
        /// <summary>
        /// 与下游同步数据
        /// </summary>
        /// <param name="infoid"></param>
        public static void SyncDatax(string infoid)
        {
            int olid = 0;
            string info = null;
            string method = "get";
            try
            {
                    info = TransferData(infoid);
                    if (null != info)
                    {
                           JObject data = JObject.Parse(info);
                
                            if (data["syncFlag"].ToString() == "0")//同步开关
                                return;
                            int point = Convert.ToInt32(data["point"].ToString());
                            int companyID = Convert.ToInt32(data["companyID"].ToString());
                            int flag = deductionPoint(companyID, point, data["ordered"].ToString());
                            if (flag == 0 && data["resultCode"].ToString() == "0")
                            {
                                olid = AddNotifyData(data);
                                if (olid > 0)
                                { 
                                    string url = data["syncUrl"].ToString();
                                    if (string.IsNullOrEmpty(url))
                                    {
                                        LogHelper.WriteLog(typeof(channelService), "============>SyncDatax[同步地址错误！]");
                                        return;
                                    } 
                                    if (!string.IsNullOrEmpty(data["syncMethod"].ToString()))
                                        method = data["syncMethod"].ToString();

                                    string param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["Mobile"].ToString() + "&Fee=" + data["Fee"].ToString() + "&Message=" + data["Message"].ToString() + "&OPType=" + data["OPType"].ToString() + "&resultCode=" + data["resultCode"].ToString();
                                   
                                    if(data["status"].ToString()=="1")
                                        param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["Mobile"].ToString() + "&MobileType=" + data["MobileType"].ToString() + "&Fee=" + data["Fee"].ToString() + "&Message=" + data["Message"].ToString() + "&OPType=" + data["OPType"].ToString() + "&senderAddress=" + data["senderAddress"].ToString() + "&resultCode=" + data["resultCode"].ToString();
                                   
                                    string result =Utils.GetService(method, url, param);
                                    if (result.IndexOf("{") > -1)
                                    {
                                        JObject json = JObject.Parse(result.ToLower());

                                        if (null != json["resultcode"] && null != json["streamingno"])
                                        {
                                            string value = json["resultcode"].ToString();
                                            if (value == "0")
                                                updateSyncStatus(json);
                                        }
                                    }
                                    else
                                        LogHelper.WriteLog(typeof(channelService), "同步返回值错误============>[" + result + "]");
                                }
                            }
                    }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), "============>SyncData同步数据时异常[" + e.ToString() + "]");
            }

        }




        public static int getAllPoint(int companyID, string message)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from [channel_2016] where companyid=" + companyID + " and resultCode=0 and datatime between " + ts.GetToday();
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), sql + "============>getAllPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        public static int getPoint(int companyID, string message)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from [channel_notify] where  companyid=" + companyID + " and resultCode=0 and StreamingNo between " + ts.GetToday().Replace("/", "");
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(channelService), sql + "============>getPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        static int deductionPoint(int companyID, int point, string message)
        {
            int point1 = getPoint(companyID, message);
            int point2 = getAllPoint(companyID, message);
            if (point1 > 0)
            {
                double data = Math.Round((double)(point2 - point1) / point2, 3);
                int i = (int)(data * 100);
                if (i < point)
                    return 1;
            }
            return 0;
        }



    }
}
