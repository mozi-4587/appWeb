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
using System.Collections;
using System.Xml;

namespace app.Public
{
    public class Service
    {

        public static void syncNotify(string tabname, DataTable dt, DataTable config)
        {
            int olid = 0;
            string method = "get";
            string param = null;
            string result = null;
            string uptab = null;
            JObject data = null;
            JObject action = new JObject();
            try
            {
                //string dinfo = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                data = getSyncData(dt);
                if (null != data)
                {
                    string ainfo = getActionInfo(data["message"].ToString(), Convert.ToInt32(data["conduitID"]));
                    if (string.IsNullOrEmpty(ainfo))
                        return;
                    action = JObject.Parse(ainfo);

                    if (action["syncFlag"].ToString() == "0")//同步开关
                        return;

                    if (data["OPType"].ToString() == "1")//判断是否退订
                        return;


                    string url = action["syncUrl"].ToString();
                    if (string.IsNullOrEmpty(url))
                    {
                        LogHelper.WriteLog(typeof(Service), "============>SyncDatax[同步地址错误！]");
                        return;
                    }
                    //int point = Convert.ToInt32(data["point"].ToString());
                    //int companyID = Convert.ToInt32(data["companyID"].ToString());
                    //int flag = deductionPoint(companyID, point, data["ordered"].ToString(), data["buyID"].ToString());
                    if (/*flag == 0 && */data["resultCode"].ToString() == "0")
                    {

                        uptab = config.Rows[0]["tablename"].ToString();

                        param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["mobile"].ToString() + "&MobileType=" + action["operators"].ToString() + "&Fee=" + data["fee"].ToString() + "&Message=" + data["message"].ToString() + "&OPType=" + data["OPType"].ToString() + "&senderAddress=" + action["address"].ToString() + "&resultCode=" + data["resultCode"].ToString();


                        DataTable field = getField("10", "4");//同步数据目前固定使用固定字段查询并插入 后续将改为设置字段
                        //getField(config.Rows[0]["infoid"].ToString(),config.Rows[0]["conduitid"].ToString());


                        olid = AddSyncData(field, tabname, config.Rows[0]["tablename"].ToString(), data["correlator"].ToString(), "syncpack", url + "?" + param);
                        if (olid > 0)
                        {
                            if (!string.IsNullOrEmpty(action["syncMethod"].ToString()))
                                method = action["syncMethod"].ToString();

                            result = Utils.GetService(method, url, param);
                            updateSyncData(data["StreamingNo"].ToString(), uptab);
                            /*if (result.IndexOf("{") == -1)
                            {
                                LogHelper.WriteLog(typeof(Service), "同步返回值错误=============================>\r\n"
                                                                    + "[" + result + "]\r\n"
                                                                    + "[" + data + "]\r\n"
                                                                    + "[" + param + "]\r\n"
                                                                    + "======================END=================\r\n");
                            }*/
                        }
                    }
                }
            }
            catch (Exception e)
            {
                updateSyncData(data["StreamingNo"].ToString(), uptab);
                LogHelper.WriteLog(typeof(Service), "============>SyncData同步数据时异常+[" + e.ToString() + "]\r\n"
                                                                    + "[" + data + "]\r\n"
                                                                    + "[" + param + "]\r\n"
                                                                    + "======================END=================\r\n"); ;
            }
        }


        public static DataTable getField(string configid, string conduitid)
        {

            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql = "select * from public_field where configid=" + configid + " and conduitid=" + conduitid;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getField获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;

        }


        public static string getActionInfo(string code, int conduitid)
        {
            string info = null;
            string sql = "SELECT top 1 a.*,b.address FROM action as a left join product as b on a.productID=b.infoid where a.conduitid=" + conduitid + " and  a.ordered =(SELECT max(ordered) FROM action where ordered=left('" + code + "',LEN(ordered)))";
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                else
                    LogHelper.WriteLog(typeof(Service), "============>getActionInfo(string code, int conduitid)获取数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(string code, int conduitid)获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }


        static JObject getSyncData(DataTable dt)
        {
            JObject data = null;
            string info = "";
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (dt.Rows[i]["fieldname"].ToString() == "notifypack")
                    continue;
                info += "," + dt.Rows[i]["fieldname"].ToString() + ":\"" + dt.Rows[i]["value"].ToString() + "\"";
            }
            if (!string.IsNullOrEmpty(info))
                data = JObject.Parse("{" + info.Substring(1) + "}");

            return data;
        }

        static void updateSyncData(string StreamingNo, string tabname)
        {
            int olid = 0;
            string tab = "public_sync_2017";//此处的值必须在sysconfig中配置
            if (!string.IsNullOrEmpty(tabname))

                try
                {
                    string sql = "update " + tab + " set result=1 where StreamingNo='" + StreamingNo + "'";
                    olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                    if (olid < 1)
                        LogHelper.WriteLog(typeof(Service), "============>updateSyncData更新数据失败[" + sql + "]");
                }
                catch (Exception e)
                {
                    LogHelper.WriteLog(typeof(Service), "============>updateSyncData更新数据异常[" + e.ToString() + "]");
                }
        }

        static int AddSyncData(DataTable dt, string gettab, string settab, string id, string packname, string packdata)
        {
            string field = null;
            int flag = 0;
            string sql = null;
            try
            {
                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow dr in dt.Rows)
                        field += "," + dr["fieldname"].ToString();

                    sql = "INSERT INTO " + settab + "(" + packname + field + ") SELECT top 1 '" + packdata + "'" + field + " from " + gettab + " where StreamingNo='" + id + "'";
                    flag = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                    if (flag == 0)
                        LogHelper.WriteLog(typeof(Service), "AddSyncData============>插入数据失败[" + sql + "]");
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>AddSyncData插入数据异常[" + e.ToString() + "]");
                return flag;
            }
            return flag;
        }


    }
}
