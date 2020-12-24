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
using System.Globalization;


namespace app.Service
{
    public class Service
    {
        public static string getFormant(string name, System.Text.Encoding code)
        {
            string info = null;
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + ConfigurationManager.AppSettings["format"] + name;
            using (FileStream fs = File.OpenRead(path))
            {
                //新建字节型数组，数组的长度是fs文件对象的长度(后面用于存放文件)
                byte[] bt = new byte[fs.Length];
                //通过fs对象的Read方法bt得到了fs对象流中的内容
                fs.Read(bt, 0, bt.Length);
                //关闭fs流对象
                fs.Close();
                //将bt字节型数组中的数据由Encoding.Default.GetString(bt)方法取出，交给textbox2.text
                info = code.GetString(bt);
            }
            return info;
        }


        public static string getFormantZH(string name, System.Text.Encoding code)
        {
            string info = null;
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + ConfigurationManager.AppSettings["format"] + name;
            using (FileStream fs = File.OpenRead(path))
            {
                //新建字节型数组，数组的长度是fs文件对象的长度(后面用于存放文件)
                byte[] bt = new byte[fs.Length];
                //通过fs对象的Read方法bt得到了fs对象流中的内容
                fs.Read(bt, 0, bt.Length);
                //关闭fs流对象
                fs.Close();
                //将bt字节型数组中的数据由Encoding.Default.GetString(bt)方法取出，交给textbox2.text
                info = Encoding.GetEncoding("GB2312").GetString(bt);
            }
            return info;
        }

        public static int setFormat(string path, string text)
        {
            int flag = 0;
            using (FileStream fs = File.Open(path, FileMode.Create))
            {
                //新建字节型数组bt对象，bt对象得到了textbox2.text的Encoding的值
                byte[] bt = System.Text.Encoding.Default.GetBytes(text);
                //将bt字节型数组对象的值写入到fs流对象中(文件)
                fs.Write(bt, 0, bt.Length);
                //关闭流对象
                fs.Close();
                flag = 1;
            }
            return flag;
        }


        public static mobileArea getMobileArea(string mobile)
        {
            mobileArea area = new mobileArea();

            if (mobile.Trim() == "" || string.IsNullOrEmpty(mobile))
            {
                area.Province = "[无号码]";
                area.City = "";
                return area;
            }
            
            string sql = "select  top 1 * from mobileArea where number=left('" + mobile.Trim() + "',7)";
           
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt.Rows.Count > 0)
                {
                    /*bool b = dt.Columns.Contains("area");

                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                       area.ID = Convert.ToInt32(dt.Rows[i]["id"].ToString());

                        if (b)
                        {
                            if (null != dt.Rows[i]["area"])
                                area.Area = dt.Rows[i]["area"].ToString();
                            else
                                area.Area = "";
                        }
                        else
                        {
                            area.Area = dt.Rows[i]["province"].ToString() + ' ' + dt.Rows[i]["city"].ToString();
                            area.Province = dt.Rows[i]["province"].ToString();
                            area.City = dt.Rows[i]["city"].ToString();
                        }*/
                    area.Province = dt.Rows[0]["Province"].ToString();
                    area.City = dt.Rows[0]["city"].ToString();
                    area.Number = dt.Rows[0]["number"].ToString();
                    area.PostCode = dt.Rows[0]["postcode"].ToString();
                    area.Type = dt.Rows[0]["type"].ToString();
                    area.AreaCode = dt.Rows[0]["areacode"].ToString();
                }
                else
                {
                    area.Province = "[未识别]";
                    area.City = "";
                }

                //}
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getMobileArea查询异常[" + e.ToString() + "]");
            }

            return area;
        }

        /// <summary>
        /// 根据指令获取产品信息
        /// </summary>
        /// <param name="code"></param>
        /// <returns></returns>
        public static string getProductInfo(string code)
        {
            string info = null;
            string sql = "SELECT top 1 a.*,b.name,b.address,b.businessID  FROM action as a left join product as b on a.productID=b.infoid where a.ordered =(SELECT max(ordered) FROM action where ordered=left('" + code + "',LEN(ordered)))";
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter());
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getProductInfo获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }



        public static string getProductInfo(int conduitid, string product)
        {
            string sql = null;
            string info = null;
            try
            {
                sql = "select top 1 a.*,b.names from product as a left join conduitid as b on b.infoid=a.conduitid"
                    + " where a.productCode='" + product + "' and a.conduitid=" + conduitid;
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getProductInfo(int conduitid, string product)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }



        public static string getProductInfo(int conduitid, int companyID, string product)
        {
            string sql = null;
            string info = null;
            try
            {
                sql = "select top 1 a.*,b.openid,b.names as conduitname,(select name from product where infoid=a.productid) as productname from action as a left join conduit as b on b.infoid=a.conduitid"
                    + " where a.productCode='" + product + "' and a.conduitid=" + conduitid+" and a.companyID="+companyID;
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getProductInfo(int conduitid, int companyID, string product)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static DataTable getProductTab(int companyID, string product)
        {
            string sql = null;
            DataTable dt = new DataTable();
;
            try
            {
                sql = "select top 1 a.*,b.name as productname,b.startFlag,(select names from conduit where infoid=a.conduitid) as conduitname from action as a left join product as b on b.infoid=a.productid"
                    + " where a.productCode='" + product + "' and a.companyID=" + companyID;

                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
               
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getProductInfo(int companyID, string product)获取数据异常[" + e.ToString() + "]");
                return dt;
            }
            return dt;
        }


        public static DataTable getProductTab(int companyID, int infoid)
        {
            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql = "select top 1 a.*,b.name as productname,b.startFlag,(select names from conduit where infoid=a.conduitid) as conduitname from action as a left join product as b on b.infoid=a.productid"
                    + " where a.infoid=" + infoid + " and a.companyID=" + companyID;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getProductInfo(int conduitid, int companyID, int infoid)获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        /// <summary>
        /// 根据产品编码获取产品信息(2016-10-16修改已停用该方法)
        /// </summary>
        /// <param name="ProductCode"></param>
        /// <returns></returns>
        public static string getFeatureStr(string ProductCode)
        {
            string info = null;
            JObject jsonObj = new JObject();
            string sql = "SELECT top 1 a.*,b.name, b.address,b.businessID FROM action as a left join product as b on a.productID=b.infoid where a.ProductCode='" + ProductCode + "' and a.parentid<>0";
            SqlDataReader dr = SqlHelper.ExecuteReader(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            try
            {

                if (dr.Read())
                {
                    jsonObj.Add(new JProperty("ordered", dr["ordered"].ToString()));
                    jsonObj.Add(new JProperty("unsubscribe", dr["unsubscribe"].ToString()));
                    jsonObj.Add(new JProperty("omessage", dr["omessage"].ToString()));
                    jsonObj.Add(new JProperty("infoid", Convert.ToInt32(dr["infoid"])));
                    jsonObj.Add(new JProperty("ProductCode", dr["ProductCode"].ToString()));
                    jsonObj.Add(new JProperty("price", dr["price"].ToString()));
                    jsonObj.Add(new JProperty("umessage", dr["umessage"].ToString()));
                    jsonObj.Add(new JProperty("productID", Convert.ToInt32(dr["productID"])));
                    jsonObj.Add(new JProperty("buyID", Convert.ToInt32(dr["buyID"])));
                    jsonObj.Add(new JProperty("companyID", Convert.ToInt32(dr["companyID"])));
                    jsonObj.Add(new JProperty("address", dr["address"].ToString()));
                    jsonObj.Add(new JProperty("name", dr["name"].ToString()));
                    jsonObj.Add(new JProperty("buyname", dr["buyname"].ToString()));
                    jsonObj.Add(new JProperty("businessID", dr["businessID"].ToString()));
                    jsonObj.Add(new JProperty("rules", dr["rules"].ToString()));
                    info = jsonObj.ToString();
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getFeatureStr读取数据异常[" + e.ToString() + "]");
            }
            finally
            {
                dr.Close();
            }
            return info;
        }

        /// <summary>
        /// 根据产品编码获取产品信息(包月业务使用)
        /// </summary>
        /// <param name="ProductCode"></param>
        /// <returns></returns>
        public static string getFeatureStrx(string ProductCode)
        {
            string info = null;
            string sql = "SELECT  top 1 * FROM action  where ProductCode='" + ProductCode + "'";
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getFeatureStrx获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        /// <summary>
        /// 更新订阅数据表(2016-11-3修改已停用该方法)
        /// </summary>
        /// <param name="infoid"></param>
        static int updateResultCode(string infoid)
        {
            int olid = 0;
            try
            {
                int number = getCompanyPoint(infoid);
                bool flag = Utils.RandNumber(number);
                string sql = "update tab_notify set resultCode='" + (flag == true ? 0 : 1) + "' where correlator='" + infoid + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), sql + "============>updateResultCode更新数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>updateResultCode数据异常[" + e.ToString() + "]");
            }

            return olid;
        }


        //获取用户额度(2016-10-16修改已停用该方法)
        public static int getCompanyPoint(string infoid)
        {
            int info = 0;
            JObject jsonObj = new JObject();
            string sql = "SELECT b.point FROM tab_notify as a left join company as b on a.companyID=b.infoid where a.correlator='" + infoid + "'";
            object obj = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            try
            {
                if (obj != System.DBNull.Value)
                    info = Convert.ToInt32(obj);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getCompanyPoint读取数据异常[" + e.ToString() + "]");
            }

            return info;
        }


        /// <summary>
        /// 获取下游同步信息
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        static string TransferData(string infoid)
        {
            string info = null;
            try
            {
                string sql = "select top 1 REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(varchar,a.datatime, 121),'-',''),':',''),'.',''),' ','') as StreamingNo,a.infoid,a.productCode,a.Mobile,a.Fee,a.Message,convert(varchar(27),a.datatime) as datatime,"
                           + "case when a.orderresult='定购' then 0 else 1 end as OPType,a.resultCode,a.companyID,a.Area,a.correlator,a.buyID,c.syncUrl,c.syncMethod,c.syncFlag,c.point,c.ordered,c.unsubscribe from tab_2016 as a WITH(NOLOCK)"
                           + " left join action as c on c.ordered=(SELECT max(ordered) FROM action where ordered=left(a.message,LEN(ordered))) or c.unsubscribe=(SELECT max(unsubscribe) FROM action where unsubscribe=left(a.message,LEN(unsubscribe)))"
                           + " where a.correlator='" + infoid + "'";
                //string sql = "select REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(varchar,a.datatime, 121),'-',''),':',''),'.',''),' ','') as StreamingNo,a.infoid,a.productCode,a.Mobile,a.Fee,a.Message,case when a.orderresult='定购' then 0 else 1 end as OPType,a.resultCode,a.companyID,a.Area,a.correlator,a.buyID,b.syncFlag,b.point from tab_2016 as a left join company as b on b.infoid=a.companyID where a.correlator='" + infoid + "'";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).ToString().Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>TransferData获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }



        /// <summary>
        /// 获取下游同步信息
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        static string TransferDataTemp(string infoid)
        {
            string info = null;
            try
            {
                string sql = "select top 1 d.address as senderAddress,c.operators as MobileType,c.status,REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(varchar,a.datatime, 121),'-',''),':',''),'.',''),' ','') as StreamingNo,a.infoid,a.productCode,a.Mobile,a.Fee,a.Message,convert(varchar(27),a.datatime) as datatime,"
                           + "case when a.orderresult='定购' then 0 else 1 end as OPType,a.resultCode,a.companyID,a.Area,a.correlator,a.buyID,c.syncUrl,c.syncMethod,c.syncFlag,c.point,c.ordered,c.unsubscribe,c.setpoint from tab_2016 as a WITH(NOLOCK)"
                           + " left join action as c on c.ordered=(SELECT max(ordered) FROM action where ordered=left(a.message,LEN(ordered))) or c.unsubscribe=(SELECT max(unsubscribe) FROM action where unsubscribe=left(a.message,LEN(unsubscribe)))"
                           + " right join product as d on d.infoid=a.productID"
                           + " where a.correlator='" + infoid + "'";
                //string sql = "select REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(varchar,a.datatime, 121),'-',''),':',''),'.',''),' ','') as StreamingNo,a.infoid,a.productCode,a.Mobile,a.Fee,a.Message,case when a.orderresult='定购' then 0 else 1 end as OPType,a.resultCode,a.companyID,a.Area,a.correlator,a.buyID,b.syncFlag,b.point from tab_2016 as a left join company as b on b.infoid=a.companyID where a.correlator='" + infoid + "'";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).ToString().Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>TransferData获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }


        /// <summary>
        /// 更新同步数据状态(2016-10-16修改已停用该方法)
        /// </summary>
        /// <param name="StreamingNo"></param>
        static void updateSyncStatus(string StreamingNo)
        {
            int olid = 0;
            try
            {
                string sql = "update tab_notify set result=1 where StreamingNo='" + StreamingNo + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), "============>updateSyncStatus更新数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>updateSyncStatus更新数据异常[" + e.ToString() + "]");
            }

        }


        /// <summary>
        /// 更新同步数据状态
        /// </summary>
        /// <param name="data"></param>
        static void updateSyncStatus(JObject data)
        {
            int olid = 0;
            try
            {
                string sql = "update tab_notify set result='" + data["resultcode"].ToString() + "' where StreamingNo='" + data["streamingno"].ToString() + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), "============>updateSyncStatus更新数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>updateSyncStatus更新数据异常[" + e.ToString() + "]");
            }

        }

        /// <summary>
        /// 更新通知表同步状态(2016-11-3修改已停用该方法)
        /// </summary>
        /// <param name="infoid"></param>
        static void ExceptionSyncStatus(string infoid)
        {
            int olid = 0;
            try
            {
                string sql = "update tab_notify set result=1 where correlator='" + infoid + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), "============>updateSyncStatus更新数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>updateSyncStatus更新数据异常[" + e.ToString() + "]");
            }

        }



        //向通知表添加数据
        public static int AddNotifyData(JObject code)
        {
            int olid = 0;
            try
            {
                string sql = "insert into tab_notify(infoid,StreamingNo,productCode,Mobile,Fee,Message,correlator,OPType,resultCode,companyID,Area,buyID,datatime)"
                          + " values(" + code["infoid"].ToString() + ",'" + code["StreamingNo"].ToString() + "','" + code["productCode"].ToString() + "','" + code["Mobile"].ToString() + "','" + code["Fee"].ToString() + "','" + code["Message"].ToString() + "','" + code["correlator"].ToString() + "','" + code["OPType"].ToString() + "','" + code["resultCode"].ToString() + "'," + code["companyID"].ToString() + ",'" + code["Area"].ToString() + "'," + code["buyID"].ToString() + ",'" + code["datatime"].ToString() + "')";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), sql + "============>AddNotifyData添加数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>AddNotifyDataa添加数据异常[" + e.ToString() + "]");
            }
            return olid;
        }

        /// <summary>
        /// 同步
        /// </summary>
        /// <param name="infoid"></param>
        public static void SyncDatax(string codeid)
        {
            int olid = 0;
            string info = null;
            string method = "get";
            string param = null;
            string result = null;
            int infoid = 0;
            int flag=0;
            string value = null;
            string find = null;
            string itempoint = null;
            JObject data = new JObject();
            string initem = string.Empty;
            try
            {
                info = TransferDataTemp(codeid);//TransferData(codeid);
                if (null != info)
                {
                    data = JObject.Parse(info);

                    if (data["syncFlag"].ToString() == "0")//同步开关
                        return;

                    if (data["unsubscribe"].ToString() == data["Message"].ToString())//判断是否退订
                        return;

                    if (data["buyID"].ToString() == "2")//判断是否有包月定购记录
                    {
                        infoid = searchMFRecord(data["Mobile"].ToString());
                        if (infoid == 0)
                            addRecord(data["infoid"].ToString(), data["Mobile"].ToString(), data["datatime"].ToString(), data["Area"].ToString());
                        else
                            return;
                    }

                    int point = Convert.ToInt32(data["point"].ToString());
                    int companyID = Convert.ToInt32(data["companyID"].ToString());

                    if (!string.IsNullOrEmpty(data["setpoint"].ToString()))
                    {
                        //string temp = "[{"field":"area","value":"湖南","param":"area like '湖南%'","point":61},{"field":"area","value":"山东","param":"area like '山东%'","point":51},{"field":"area","value":"江西","param":"area like '江西%'","point":51}]";
                        string json = "[" + data["setpoint"].ToString() + "]";
                        JArray setpoint = JArray.Parse(json);
                        foreach (JObject items in setpoint)
                        {
                            string field = getJsonFind(data, items["field"].ToString());
                           
                            find = items["value"].ToString();
                            value = data[field].ToString();
                            if (value.IndexOf(find) > -1)
                            {
                                itempoint = items.ToString();
                                break;
                            }
                            initem += " and " + field + " not like '" + find + "%'";
                        }
                    }

                    if (!string.IsNullOrEmpty(find) && !string.IsNullOrEmpty(itempoint))
                        flag = deductionPoint(companyID, data["ordered"].ToString(), data["buyID"].ToString(), itempoint);
                    else
                        flag = deductionPoint(companyID, data["ordered"].ToString(), data["buyID"].ToString(), initem.Substring(4), point);
                        //flag = deductionPoint(companyID, point, data["ordered"].ToString(), data["buyID"].ToString());
                    

                    if (flag == 0 && data["resultCode"].ToString() == "0")
                    {

                        olid =AddNotifyData(data);
                        if (olid > 0)
                        {
                            string url = data["syncUrl"].ToString();
                            if (string.IsNullOrEmpty(url))
                            {
                                LogHelper.WriteLog(typeof(Service), "============>SyncDatax[同步地址错误！]");
                                return;
                            }
                            if (!string.IsNullOrEmpty(data["syncMethod"].ToString()))
                                method = data["syncMethod"].ToString();

                            param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["Mobile"].ToString() + "&Fee=" + data["Fee"].ToString() + "&Message=" + data["Message"].ToString() + "&OPType=" + data["OPType"].ToString() + "&resultCode=" + data["resultCode"].ToString();
                            if (data["status"].ToString() == "1")
                                param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["Mobile"].ToString() + "&MobileType=" + data["MobileType"].ToString() + "&Fee=" + data["Fee"].ToString() + "&Message=" + data["Message"].ToString() + "&OPType=" + data["OPType"].ToString() + "&senderAddress=" + data["senderAddress"].ToString() + "&resultCode=" + data["resultCode"].ToString();
                            result =Utils.GetService(method, url, param);

                            /*if (result.IndexOf("{") > -1)
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
                            {
                                LogHelper.WriteLog(typeof(Service), "同步返回值错误============>\r\n"
                                                                    + " [" + result + "]\r\n"
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
                updateSyncStatus(data["StreamingNo"].ToString());
                LogHelper.WriteLog(typeof(Service), "============>SyncData同步数据时异常+[" + e.ToString() + "]\r\n"
                                                                    + "[" + data + "]\r\n"
                                                                    + "[" + param + "]\r\n"
                                                                    + "======================END=================\r\n"); ;
            }
        }



        public static string getJsonFind(JObject data,string field)
        {
            string info = null;
            IEnumerable<JProperty> properties = data.Properties();
            foreach (JProperty item in properties)
            {
                string value=item.Name.ToLower();
                if (value == field.ToLower())
                    info = item.Name;
            }

            return info;
        }

        public static void SyncDataTemp(string codeid)
        {
            int olid = 0;
            string info = null;
            string method = "get";
            string param = null;
            string result = null;
            int infoid = 0;
            JObject data = new JObject();
            try
            {
                info = TransferDataTemp(codeid);
                if (null != info)
                {
                    data = JObject.Parse(info);

                    if (data["syncFlag"].ToString() == "0")//同步开关
                        return;

                    if (data["unsubscribe"].ToString() == data["Message"].ToString())//判断是否退订
                        return;

                    if (data["buyID"].ToString() == "2")//判断是否有包月定购记录
                    {
                        infoid = searchMFRecord(data["Mobile"].ToString());
                        if (infoid == 0)
                            addRecord(data["infoid"].ToString(), data["Mobile"].ToString(), data["datatime"].ToString(), data["Area"].ToString());
                        else
                            return;
                    }

                    int point = Convert.ToInt32(data["point"].ToString());
                    int companyID = Convert.ToInt32(data["companyID"].ToString());
                    int flag = deductionPoint(companyID, point, data["ordered"].ToString(), data["buyID"].ToString());
                    if (flag == 0 && data["resultCode"].ToString() == "0")
                    {

                        olid = AddNotifyData(data);
                        if (olid > 0)
                        {
                            string url = data["syncUrl"].ToString();
                            if (string.IsNullOrEmpty(url))
                            {
                                LogHelper.WriteLog(typeof(Service), "============>SyncDatax[同步地址错误！]");
                                return;
                            }
                            if (!string.IsNullOrEmpty(data["syncMethod"].ToString()))
                                method = data["syncMethod"].ToString();


                            param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["Mobile"].ToString() + "&MobileType=" + data["MobileType"].ToString() + "&Fee=" + data["Fee"].ToString() + "&Message=" + data["Message"].ToString() + "&OPType=" + data["OPType"].ToString() + "&senderAddress=" + data["senderAddress"].ToString() + "&resultCode=" + data["resultCode"].ToString();
                            result = Utils.GetService(method, url, param);
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
                            {
                                LogHelper.WriteLog(typeof(Service), "同步返回值错误============>\r\n"
                                                                    + " [" + result + "]\r\n"
                                                                    + "[" + data + "]\r\n"
                                                                    + "[" + param + "]\r\n"
                                                                    + "======================END=================\r\n");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>SyncData同步数据时异常+[" + e.ToString() + "]\r\n"
                                                                    + "[" + data + "]\r\n"
                                                                    + "[" + param + "]\r\n"
                                                                    + "======================END=================\r\n"); ;
            }
        }


        /// <summary>
        /// 添加包月卡号历史记录
        /// </summary>
        /// <param name="code"></param>
        public static void addRecord(string infoid, string mobile, string datatime, string area)
        {
            int olid = 0;
            string sql = "INSERT INTO record_info(infoid,mobile,datatime,area) values(" + infoid + ",'" + mobile + "','" + datatime + "','" + area + "')";

            try
            {
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), sql + "============>addRecord添加数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>addRecord[" + e.ToString() + "]");

            }


        }


        /// <summary>
        /// 同步数据
        /// </summary>
        /// <param name="infoid"></param>
        public static void SyncData(string infoid)
        {
            /*JArray data = new JArray();
            string param = "";
            try
            {
               int olid= updateResultCode(infoid);
               if (olid > 0)
               {
                   data = JArray.Parse(TransferData(infoid));
                   if (data.Count > 0)
                   {
                       int flag = Convert.ToInt32(data[0]["syncFlag"].ToString());
                       if (flag != 1)//同步开关
                           return;
                       string url = data[0]["syncUrl"].ToString();
                       if(string.IsNullOrEmpty(url))
                       {
                          
                           LogHelper.WriteLog(typeof(Service), "============>SyncData[同步地址错误！]");
                           return ;
                       }
                       JObject o = (JObject)data[0];

                       o.Remove("syncUrl");

                       IEnumerable<JProperty> properties = o.Properties();  
                       foreach (JProperty item in properties)  
                       {
                           if (string.IsNullOrEmpty(item.Value.ToString()))
                           {
                               LogHelper.WriteLog(typeof(Service), "============>SyncData同步数据参数错误[" + item.Name + "+值为空]");
                               return;
                           }
                           else
                               param += "&" + item.Name + "=" + item.Value;

                       }
                       //param = o.ToString().Replace(":", "=").Replace(",", "&");
                       string result = Utils.GetService("post", url, param.Substring(1));
                       if (result.IndexOf("{") > -1)
                       {
                           JObject json = JObject.Parse(result);
                           string value = json["resultCode"].ToString();
                           if (value != "0")
                               updateSyncStatus(json["streamingNo"].ToString());
                       }
                   }
               }*/
            try
            {
                SyncDatax(infoid);
            }
            catch (Exception e)
            {
                //ExceptionSyncStatus(infoid);
                LogHelper.WriteLog(typeof(Service), "============>SyncData同步数据时异常[" + e.ToString() + "]");
            }
        }

        //解析点播数据包
        public static int ParsingSFJsonData(string data, ref string mobile, ref int fee)
        {
            int flag = -1;
            try
            {
                JObject jsonObj = JObject.Parse(data);

                mobile = jsonObj["body"]["message"]["senderAddress"].ToString();
                string message = jsonObj["body"]["message"]["message"].ToString();

                string info = getProductInfo(message);
                JArray jsondata = JArray.Parse(info.ToLower());
                if (jsondata[0]["syncstart"].ToString() == "0")
                    return flag;
                fee = Convert.ToInt32(jsondata[0]["price"].ToString());
                return Convert.ToInt32(jsondata[0]["rulesid"].ToString());

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>ParsingSFJsonData解析异常[" + e.ToString() + "]");
                return flag;
            }

        }

        //解析包月数据包
        public static int ParsingMFJsonData(string data, ref string mobile)
        {
            int flag = -1;
            try
            {
                JObject jsonObj = JObject.Parse(data);

                string info = Service.getFeatureStrx(jsonObj["body"]["productId"].ToString());
                JObject jsondata = JObject.Parse(info.ToLower());
                mobile = jsonObj["body"]["userId"].ToString();
                if (jsondata[0]["syncstart"].ToString() == "0")
                    return flag;
                return Convert.ToInt32(jsondata["rulesid"].ToString());

            }
            catch (Exception e)
            {

                LogHelper.WriteLog(typeof(Service), "============>ParsingMFJsonData解析异常[" + e.ToString() + "]");
                return -1;
            }

        }

        /// <summary>
        /// 获取业务规则数据(该方法返回一个json，为了明确返回格式 后期应该命名为getJsonRules)
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string getRules(int id)
        {
            string info = null;
            string sql = "SELECT * FROM rules where infoid=" + id;
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getRules获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }




        public static string getRules(int id,string field)
        {
            string info = null;
            string sql = "SELECT " + field + " FROM rules where infoid=" + id;
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = dt.Rows[0][field].ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getRules(int id,string field)获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }



        public static DataTable getRules(int productid, int actionid)
        {
            DataTable dt=new DataTable();
            string sql = string.Format("SELECT limit, dayfee, monthfee, timeNo, flag, typeid, pid, productid, actionid FROM rules where productid={0} and actionid in(0,{1})",productid,actionid);
            try
            {
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                
            }
            catch (Exception e)
            {
                //LogHelper.WriteLog(typeof(Service), sql + "============>getRules(int productid, int actionid)获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        public static DataTable getJsonRules(int productid)
        {
            return getJsonRules(productid, null);
        }

        public static DataTable getJsonRules(int productid,string typelist)
        {
            DataTable dt = new DataTable();
            string sql = string.Format("SELECT limit, typeid, pid, productid, companyid, actionid FROM rules where productid={0})", productid);
            
            if (!string.IsNullOrEmpty(typelist))
             sql = string.Format("SELECT limit, typeid, pid, productid, companyid, actionid FROM rules where productid={0} and typeid in({1}))", productid, typelist);

            try
            {
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                return dt;
            }

            return dt;
        }

        public static DataTable getRulesGroup(int productid)
        {
            DataTable dt = new DataTable();

            string sql = string.Format("SELECT typeid FROM rules where productid={0} group by typeid)", productid);

            try
            {
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                //LogHelper.WriteLog(typeof(Service), sql + "============>getRules(int productid, int actionid)获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }



        /// <summary>
        /// 查询黑名单数据
        /// </summary>
        /// <param name="mobile"></param>
        /// <returns></returns>
        public static int getBlackList(string mobile, int rulesID)
        {
            int info = 0;
            string sql = "SELECT  count(mobile) FROM blacklist where mobile='" + mobile + "' and rulesID=" + rulesID;
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getBlackList获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;

        }


        public static int getBlackList(string mobile)
        {
            int info = 0;
            string sql = "SELECT count(infoid) FROM blacklist where mobile='" + mobile + "'";
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getBlackList获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;

        }




        /// <summary>
        /// 包月业务规则
        /// </summary>
        /// <param name="mobile"></param>
        /// <param name="rules"></param>
        /// <returns></returns>
        public static bool ShieldedRules(string mobile, int rules)
        {
            bool flag = false;
            string info = getRules(rules);
            if (null != info)
            {
                JObject jsonObj = JObject.Parse(info);
                if (!string.IsNullOrEmpty(jsonObj["area"].ToString()))
                {
                    mobileArea areainfo = getMobileArea(mobile);
                    string[] area = areainfo.Area.Split(' ');
                    if (string.IsNullOrEmpty(areainfo.Area.Trim()))
                        return true;
                    if (jsonObj["area"].ToString().IndexOf(area[0]) == -1)
                        return true;
                }
                if (jsonObj["mobile"].ToString() != "0")
                {
                    int number = getBlackList(mobile);
                    if (number > 0)
                        return true;
                }
                /*if (!string.IsNullOrEmpty(jsonObj["dayfee"].ToString()))
                {
                    int dayfee = Convert.ToInt32(jsonObj["dayfee"].ToString());
                    int fee = DayFee(mobile);
                    if (fee >= dayfee)
                        return true;
                }*/
            }
            return flag;
        }

        //点播业务规则
        public static bool ShieldedRules(string mobile, int rules, int buyfee)
        {
            bool flag = false;
            string info = getRules(rules);
            if (null != info)
            {
                JObject jsonObj = JObject.Parse(info);
                if (jsonObj["mobile"].ToString() != "0")
                {
                    int number = getBlackList(mobile);
                    if (number > 0)
                        return true;
                }
                if (!string.IsNullOrEmpty(jsonObj["number"].ToString()))
                {
                    string[] items = jsonObj["number"].ToString().Split(',');
                    foreach (var p in items)
                    {
                        if (mobile.Substring(0, p.Length).IndexOf(p) > -1)
                            return true;
                    }
                }
                if (!string.IsNullOrEmpty(jsonObj["area"].ToString()))
                {
                    mobileArea areainfo = getMobileArea(mobile);
                    if (string.IsNullOrEmpty(areainfo.Area.Trim()))
                        return true;
                    string[] area = areainfo.Area.Split(' ');

                    if (jsonObj["area"].ToString().IndexOf(area[0]) == -1)
                        return true;
                }
                if (!string.IsNullOrEmpty(jsonObj["dayfee"].ToString()))
                {
                    int dayfee = Convert.ToInt32(jsonObj["dayfee"].ToString());
                    int fee = DayFee(mobile);
                    if (fee >= dayfee)
                        return true;
                }
                if (!string.IsNullOrEmpty(jsonObj["monthfee"].ToString()))
                {
                    int monthfee = Convert.ToInt32(jsonObj["monthfee"].ToString());
                    int fee = MonthFee(mobile);
                    if (fee >= monthfee)
                        return true;
                }
                if (!string.IsNullOrEmpty(jsonObj["timeNo"].ToString()))
                {
                    string[] ls = jsonObj["timeNo"].ToString().Split(',');
                    DateTime dt1 = Convert.ToDateTime(ls[0]);
                    DateTime dt2 = Convert.ToDateTime(ls[1]);
                    TimeSpan ts1 = dt1.Subtract(DateTime.Now);
                    TimeSpan ts2 = dt2.Subtract(DateTime.Now);
                    if (ts1.TotalSeconds < 0 && ts2.TotalSeconds > 1)
                        return true;
                }
            }

            return flag;
        }




        /// <summary>
        /// 查询日订购额度
        /// </summary>
        /// <param name="mobile"></param>
        /// <returns></returns>
        public static int DayFee(string mobile)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select sum(fee) as money from [tab_2016] WITH(NOLOCK) where mobile='" + mobile + "' and resultCode=0 and datatime between " + ts.GetToday();
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>DayFee获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }


        /// <summary>
        /// 查询月订购额度
        /// </summary>
        /// <param name="mobile"></param>
        /// <returns></returns>
        public static int MonthFee(string mobile)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select sum(fee) as money from [tab_2016] WITH(NOLOCK) where mobile='" + mobile + "' and resultCode=0  and datatime between " + ts.GetMonth();
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>MonthFee获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }

        /// <summary>
        /// 获取用户定购金额
        /// </summary>
        /// <returns></returns>
        public static int getAllPoint(int companyID, string message, string buyid)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from [tab_2016] WITH(NOLOCK) where status like '成功%' and companyid=" + companyID + " and message like '" + message + "%' and buyid=" + buyid + " and resultCode=0 and datatime between " + ts.GetToday();
            if (buyid == "2")
                sql = "select count(record_info.infoid) as point from tab_2016 WITH(NOLOCK) left join record_info on record_info.infoid=tab_2016.infoid where status like '成功%' and companyid=" + companyID + " and buyid=" + buyid + " and resultCode=0 and tab_2016.datatime between " + ts.GetToday();
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getAllPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        public static int getAllPoint(int companyID, string message, string buyid, string param)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from [tab_2016] WITH(NOLOCK) where status like '成功%' and companyid=" + companyID + " and message like '" + message + "%' and buyid=" + buyid + " and resultCode=0 and datatime between " + ts.GetToday() + " and " + param;
            if (buyid == "2")
                sql = "select count(record_info.infoid) as point from tab_2016 WITH(NOLOCK) left join record_info on record_info.infoid=tab_2016.infoid where status like '成功%' and companyid=" + companyID + " and buyid=" + buyid + " and resultCode=0 and tab_2016.datatime between " + ts.GetToday() + " and tab_2016." + param;
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getAllPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        

        public static int getPoint(int companyID, string message, string buyid)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from [tab_notify] where companyid=" + companyID + " and message like '" + message + "%' and buyid=" + buyid + " and resultCode=0 and StreamingNo between " + ts.GetToday().Replace("-", "");
            if (buyid == "2")
                sql = "select count(infoid) as point from [tab_notify] where companyid=" + companyID + " and buyid=" + buyid + " and resultCode=0 and StreamingNo between " + ts.GetToday().Replace("-", "");
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        public static int getPoint(int companyID, string message, string buyid ,string param)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from [tab_notify] where companyid=" + companyID + " and message like '" + message + "%' and buyid=" + buyid + " and resultCode=0 and StreamingNo between " + ts.GetToday().Replace("-", "") + " and " + param;
            if (buyid == "2")
                sql = "select count(infoid) as point from [tab_notify] where companyid=" + companyID + " and buyid=" + buyid + " and resultCode=0 and StreamingNo between " + ts.GetToday().Replace("-", "") + " and " + param;
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }




        static int deductionPoint(int companyID, int point, string message, string buyid)
        {
            int point1 = getPoint(companyID, message, buyid);
            int point2 = getAllPoint(companyID, message, buyid);
            if (point1 > 0)
            {
                double data = Math.Round((double)(point2 - point1) / point2, 3);
                int i = (int)(data * 100);
                if (i < point)
                    return 1;
            }
            return 0;
        }

        static int deductionPoint(int companyID, string message, string buyid, string param)
        {
            JObject info= JObject.Parse(param);
            int point = Convert.ToInt32(info["point"].ToString());
            int point1 = getPoint(companyID, message, buyid, info["param"].ToString());
            int point2 = getAllPoint(companyID, message, buyid, info["param"].ToString());
            if (point1 > 0)
            {
                double data = Math.Round((double)(point2 - point1) / point2, 3);
                int i = (int)(data * 100);
                if (i < point)
                    return 1;
            }
            return 0;
        }


        static int deductionPoint(int companyID, string message, string buyid, string param, int point)
        {
          
           ;
            int point1 = getPoint(companyID, message, buyid, param);
            int point2 = getAllPoint(companyID, message, buyid, param);
            if (point1 > 0)
            {
                double data = Math.Round((double)(point2 - point1) / point2, 3);
                int i = (int)(data * 100);
                if (i < point)
                    return 1;
            }
            return 0;
        }

        
        

        /// <summary>
        /// 查询同步包月号码
        /// </summary>
        /// <param name="mobile"></param>
        /// <returns></returns>
        public static int SearchMFnotify(string data)
        {
            int info = 0;
            string sql = null;

            try
            {
                JObject jsonObj = JObject.Parse(data);

                sql = "select count(infoid) as num from [tab_notify] WITH(NOLOCK) where mobile='" + jsonObj["body"]["userId"].ToString() + "' and buyid=2";
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>SearchMFnotify获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }


        /// <summary>
        /// 检索包月历史号码
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        static int searchMFRecord(string mobile)
        {
            string sql = "select top 1 infoid from record_info where mobile='" + mobile + "'";
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchMFRecord获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        /*-----------------------------------------------2017 业务接口---------------------------------------------------*/

        /// <summary>
        /// 获取指定业务步骤配置信息
        /// </summary>
        /// <param name="condutiid"></param>
        /// <param name="step"></param>
        /// <returns></returns>
        public static DataTable getConfigInfo(int infoid)
        {
            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql = "select top 1 * from public_Config where infoid=" + infoid;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getConfigInfo获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }

        public static DataTable getConfigInfo(int infoid, int step)
        {
            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql = "select top 1 * from public_Config where pid=" + infoid + " and step=" + (step + 1);
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getConfigInfo获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        public static DataTable getConfigInfo(int conduitid,int groupid,int step)
        {
            string sql = null;
            string param = string.Empty;
            DataTable dt = new DataTable();
            try
            {
                if (groupid > 0)
                    param += " and groupid=" + groupid;
                if (step > 0)
                    param += " and step=" + step;
                
                sql = "select * from public_Config where conduitid=" + conduitid + param;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getConfigInfo(int conduitid,int groupid,int step)获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        public static string FindXmlNode(XmlDocument xmlDoc, string node)
        {

            XmlNode xmlNode = xmlDoc.SelectSingleNode(node);
            if (null != xmlNode)
                return xmlNode.InnerText;
            else
                return null;
        }


        public static string FindXmlNode(string str, string node)
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(str);
            XmlNode xmlNode = xmlDoc.SelectSingleNode(node);
            if (null != xmlNode)
                return xmlNode.InnerText;
            else
                return null;
        }

        public static string FindJsonNode(string json, string node)
        {
            int spoint = json.IndexOf(node);
            if (spoint < 0)
                return null;
            string str = json.Substring(spoint);
            int epoint = str.IndexOf(",");
            string data = json.Substring(spoint, epoint);

            return data.Substring(data.IndexOf(":") + 1).Replace("\"", "");
        }

        public static string FindJsonNodex(string json, string node)
        {
            string data = string.Empty;
            int spoint = json.IndexOf(node);
            if (spoint < 0)
                return null;
            string str = json.Substring(spoint);
            int epoint = str.IndexOf(",");
            if(epoint==-1)
                data = json.Substring(spoint, (str.Length - 1));
            else
                data = json.Substring(spoint, epoint);

            return data.Substring(data.IndexOf(":") + 1).Replace("\"", "").Trim();
        }

        public static string FindJsonNodex(string json, string node, string value)
        {
            string data = string.Empty;
            int spoint = json.IndexOf(node);
            if (spoint < 0)
                return null;
            string str = json.Substring(spoint);
            int epoint = str.IndexOf(",");
            if (epoint == -1)
                data = json.Substring(spoint, (str.Length - 1));
            else
                data = json.Substring(spoint, epoint);
            JObject redata = JObject.Parse("{" + data + "}");
            redata["node"] = value;
            return json.Replace(data, redata["node"].ToString()).Trim();
        }



        public static string FindJsonNode(string json, string node, string value)
        {
            string data = string.Empty;
            int spoint = json.IndexOf(node);
            if (spoint < 0)
                return string.Empty;
            string str = json.Substring(spoint);
            int epoint = str.IndexOf(",");
            if (epoint == -1)
                data = json.Substring(spoint-1, (str.Length));
            else
                data = json.Substring(spoint-1, epoint+1);

            if (data.IndexOf("}") > -1)
                data = data.Substring(0, data.Length - 1);
            string redata = "\"" + node + "\":\"" + value + "\"";

            return json.Replace(data, redata).Trim();
        }



        public static string setJsonNode(string json ,string node, string value)
        {
            string data = string.Empty;
            JObject redata=new JObject();
            if (string.IsNullOrEmpty(json))
                redata = new JObject(new JProperty(node, value));
            else
            {
                redata = JObject.Parse(json);
                redata.Add(new JProperty(node, value));
            }
            
            return json.ToString();
        }


        public static string StreamingNo()
        {
            string tradeno = null;
            string sql = "select REPLACE(REPLACE(REPLACE(REPLACE(convert(varchar(50), sysdatetime()),'-',''),':',''),' ',''),'.','') as trade_no";

            object obj = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            try
            {
                tradeno = obj.ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>StreamingNo获取数据异常[" + e.ToString() + "]");
            }

            return tradeno;
        }

        public static void parsedData(string name, XmlDocument obj, DataTable field)
        {
            foreach (DataRow dr in field.Rows)
            {
                if (!string.IsNullOrEmpty(dr["paramname"].ToString()))
                {
                    string value = FindXmlNode(obj, dr["paramname"].ToString());

                    if (!string.IsNullOrEmpty(value))
                    {
                        /*if (!string.IsNullOrEmpty(dr["converted"].ToString()))
                        {
                            JObject json = JObject.Parse(dr["converted"].ToString());
                            value = json[value].ToString();
                        }*/

                        if (!string.IsNullOrEmpty(name))
                        {
                            if (name == dr["packname"].ToString())
                                dr["value"] = value;
                        }
                        else
                            dr["value"] = value;
                    }
                }
                
            }
        }

        public static void parsedData(string name, string obj, DataTable field)
        {
            foreach (DataRow dr in field.Rows)
            {
                if (!string.IsNullOrEmpty(dr["paramname"].ToString()))
                {
                    string value = FindJsonNodex(obj, dr["paramname"].ToString());
                    if (!string.IsNullOrEmpty(value))
                    {
                        if (!string.IsNullOrEmpty(dr["converted"].ToString()))
                        {
                            JObject json = JObject.Parse(dr["converted"].ToString());
                         
                            if (null != json[value])
                                value = json[value].ToString();
                            else if (null != json["other"])
                                value = json["other"].ToString();
                        }
                        if (dr["fee"].ToString() == "1")
                            value = value.Substring(0, value.Length - 2);

                        if (dr["StreamingNo"].ToString() == "1")
                            value = StreamingNo();

                        if (!string.IsNullOrEmpty(name))
                        {
                            if (name == dr["packname"].ToString())
                                dr["value"] = value;
                        }
                        else
                            dr["value"] = value;
                    }
                }
                else
                {
                    if (dr["StreamingNo"].ToString() == "1")
                        dr["value"] = StreamingNo();
                    if (string.IsNullOrEmpty(dr["defvalue"].ToString()))
                        dr["value"] = dr["defvalue"].ToString();
                }
            }


        }

        public static void parsedData(DataTable field, packInfo pack)
        {

            foreach (DataRow dr in field.Rows)
            {
                if (!string.IsNullOrEmpty(dr["paramname"].ToString()))
                {
                    string value = pack.getValue(dr["paramname"].ToString().ToLower());
                    if (null != value)
                        dr["value"] = value;
                }
                string fvalue = pack.getValue(dr["fieldname"].ToString().ToLower());
                if (null != fvalue)
                    dr["value"] = fvalue;
            }
        }


   

        /// <summary>
        /// 解析返回的json数据包
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="field"></param>
        public static void parsedResultPack(string obj, DataTable field)
        {
            DataRow[] dr = field.Select("parsing=1");
            for(int i=0;i<dr.Length;i++)
            {
                if (!string.IsNullOrEmpty(dr[i]["paramname"].ToString()))
                {
                    string value = FindJsonNode(obj, dr[i]["paramname"].ToString());
                    if (!string.IsNullOrEmpty(value))
                        dr[i]["value"] = value;
                }
            }
         }


        public static void parsedResultPackx(string obj, DataTable field)
        {
            DataRow[] dr = field.Select("parsing=1");
            for (int i = 0; i < dr.Length; i++)
            {
                if (!string.IsNullOrEmpty(dr[i]["paramname"].ToString()))
                {
                    string value = FindJsonNodex(obj, dr[i]["paramname"].ToString());
                    if (!string.IsNullOrEmpty(value))
                        dr[i]["value"] = value.Replace("{","").Replace("}","");
                }
            }
        }


        public static void parsedResultPackxx(string obj, DataTable field)
        {
            DataRow[] dr = field.Select("parsing=1");
            for (int i = 0; i < dr.Length; i++)
            {
                if (!string.IsNullOrEmpty(dr[i]["paramname"].ToString()))
                {
                    string value = FindJsonNodex(obj, dr[i]["paramname"].ToString());
                    if (!string.IsNullOrEmpty(value))
                        dr[i]["value"] = value.Replace("{", "").Replace("}", "");
                    else
                        dr[i]["value"] = "";
                }
            }
        }

        public static void parsedResultPackx(string obj, DataRow[] dr)
        {
            //DataRow[] dr = field.Select("parsing=1");
            for (int i = 0; i < dr.Length; i++)
            {
                if (!string.IsNullOrEmpty(dr[i]["paramname"].ToString()))
                {
                    string value = FindJsonNodex(obj, dr[i]["paramname"].ToString());
                    if (!string.IsNullOrEmpty(value))
                        dr[i]["value"] = value.Replace("{", "").Replace("}", "");
                }
            }
        }

        public static void parsedResultPackXml(string datapack, DataTable field)
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(datapack);
            parsedData(null, xmlDoc, field);
        }



        public static DataTable setpService(DataTable config, packInfo pack)
        {
            string datapack = pack.dataPack;

            Hashtable ht = pack.getAllParameters();
            ht.Add("urlpack", pack.urlpack);
            ht.Add("conduitid", config.Rows[0]["conduitid"].ToString());

            DataTable dt = getParsedParam(config.Rows[0]["infoid"].ToString());
            DataTable field = getField(config.Rows[0]["infoid"].ToString(), config.Rows[0]["conduitid"].ToString());
            field.Columns.Add("value", typeof(string));
            field.Columns.Add("err", typeof(string));
            if (config.Rows[0]["parsed"].ToString() == "1")//采用据数据包的方式，需要进行内容解析
            {

                if (dt.Rows.Count > 0)//数据包用参数传递
                {

                    foreach (DataRow dr in dt.Rows)
                    {
                        string packname = dr["packname"].ToString();
                        //string datapack=ht[packname].ToString();
                        if (dr["format"].ToString() == "1")
                        {
                            XmlDocument xmlDoc = new XmlDocument();
                            xmlDoc.LoadXml(datapack);
                            parsedData(packname, xmlDoc, field);
                        }
                        else if (dr["format"].ToString() == "0")
                            parsedData(packname, datapack, field);
                    }
                }
                else //数据包传递
                {
                    if (config.Rows[0]["format"].ToString() == "1")
                    {
                        XmlDocument xmlDoc = new XmlDocument();
                        xmlDoc.LoadXml(datapack);
                        parsedData(null, xmlDoc, field);
                    }
                    else if (config.Rows[0]["format"].ToString() == "0")
                        parsedData(null, datapack, field);
                }


                DataRow[] mobile = field.Select("area=1");//查询
                DataRow[] Streaming = field.Select("fieldname='StreamingNo'");

                DataRow[] message = field.Select("fieldname='message'");//查询
                string result = Service.getActionInfo(message[0]["value"].ToString(), Convert.ToInt32(config.Rows[0]["conduitid"]));
                if (null == result)
                    return new DataTable();
                JObject json = JObject.Parse(result);
                json.Add(new JProperty("correlator", GUID.CreatGUID("N").ToUpper()));
                if (mobile.Length > 0)
                {
                    mobileArea area = getMobileArea(mobile[0]["value"].ToString());
                    json.Add(new JProperty("area", area.Area));
                }
                if (config.Rows[0]["step"].ToString() == "3")
                {
                    json.Add(new JProperty("notifypack", datapack));
                }

                json.Add(new JProperty("StreamingNo", StreamingNo()));

                foreach (DataRow dr in field.Rows)
                {
                    string name = dr["fieldname"].ToString().ToLower();
                    string value = getJsonValue(name, json);
                    if (null != value && string.IsNullOrEmpty(dr["value"].ToString()))
                        dr["value"] = value;
                }

            }
            else
            {
                foreach (DataRow dr in field.Rows)
                {
                    if (!string.IsNullOrEmpty(dr["paramname"].ToString()))
                    {
                        string value = pack.getValue(dr["paramname"].ToString().ToLower());
                        if (null != value)
                            dr["value"] = value;
                    }
                    else
                    {
                        string value = pack.getValue(dr["fieldname"].ToString().ToLower());
                        if (null != value)
                            dr["value"] = value;
                    }
                }

            }

            return field;
        }


        public static DataTable setpServiceX(DataTable config, packInfo pack)
        {
            string datapack = pack.dataPack;
            Hashtable ht = pack.getAllParameters();

            if (config.Rows[0]["step"].ToString() == "1")
                ht.Add("urlpack", pack.urlpack);
            else if (config.Rows[0]["step"].ToString() == "2")
            {
                if (!string.IsNullOrEmpty(pack.dataPack))
                    ht.Add("uppack", pack.dataPack);
                else
                    ht.Add("uppack", pack.urlpack);
            }

            else if (config.Rows[0]["step"].ToString() == "3")
            {
                if (!string.IsNullOrEmpty(pack.dataPack))
                    ht.Add("notifypack", pack.dataPack);
                else
                    ht.Add("notifypack", pack.urlpack);
            }

            DataTable dt = getParsedParam(config.Rows[0]["infoid"].ToString());
            DataTable field = getField(config.Rows[0]["infoid"].ToString(), config.Rows[0]["conduitid"].ToString());
            field.Columns.Add("value", typeof(string));

            if (config.Rows[0]["parsed"].ToString() == "1")//采用据数据包的方式，需要进行内容解析
            {
                if (dt.Rows.Count > 0)//数据包用参数传递
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        string packname = dr["packname"].ToString();
                       
                        if (dr["format"].ToString() == "1")
                        {
                            XmlDocument xmlDoc = new XmlDocument();
                            xmlDoc.LoadXml(datapack);
                            parsedData(packname, xmlDoc, field);
                        }
                        else if (dr["format"].ToString() == "0")
                            parsedData(packname, datapack, field);
                    }
                }
                else //数据包传递
                {
                    if (config.Rows[0]["format"].ToString() == "1")
                    {
                        XmlDocument xmlDoc = new XmlDocument();
                        xmlDoc.LoadXml(datapack);
                        parsedData(null, xmlDoc, field);
                    }
                    else if (config.Rows[0]["format"].ToString() == "0")
                        parsedData(null, datapack, field);
                    parsedData(field, pack);
                }
            }
            else
                parsedData(field, pack);

            return field;
        }


        public static DataTable setpServiceXI(DataTable config, packInfo pack)
        {
            string datapack = pack.dataPack;
            Hashtable ht = pack.getAllParameters();

            if (config.Rows[0]["step"].ToString() == "1")
            {
                if (!string.IsNullOrEmpty(pack.urlpack))
                    ht.Add("userpack", pack.urlpack);

                ht.Add("urlpack", pack.urlpack);
            }
            else if (config.Rows[0]["step"].ToString() == "2")
            {
                if (!string.IsNullOrEmpty(pack.urlpack))
                   ht.Add("userpack", pack.urlpack);
            }

            else if (config.Rows[0]["step"].ToString() == "3")
            {
                if (!string.IsNullOrEmpty(pack.dataPack))
                    ht.Add("notifypack", pack.dataPack);
                else
                    ht.Add("notifypack", pack.urlpack);
            }

            DataTable dt = getParsedParam(config.Rows[0]["infoid"].ToString());
            DataTable field = getField(config.Rows[0]["infoid"].ToString(), config.Rows[0]["conduitid"].ToString());
            field.Columns.Add("value", typeof(string));

            if (config.Rows[0]["parsed"].ToString() == "1")//采用据数据包的方式，需要进行内容解析
            {
                if (dt.Rows.Count > 0)//数据包用参数传递
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        string packname = dr["packname"].ToString();
                        //string datapack=ht[packname].ToString();
                        if (dr["format"].ToString() == "1")
                        {
                            XmlDocument xmlDoc = new XmlDocument();
                            xmlDoc.LoadXml(datapack);
                            parsedData(packname, xmlDoc, field);
                        }
                        else if (dr["format"].ToString() == "0")
                            parsedData(packname, datapack, field);
                    }
                }
                else //数据包传递
                {
                    if (config.Rows[0]["format"].ToString() == "1")
                    {
                        XmlDocument xmlDoc = new XmlDocument();
                        xmlDoc.LoadXml(datapack);
                        parsedData(null, xmlDoc, field);
                    }
                    else if (config.Rows[0]["format"].ToString() == "0")
                        parsedData(null, datapack, field);
                    parsedData(field, pack);
                }
            }
            else
                parsedData(field, pack);

            return field;
        }


        /// <summary>
        /// 设置上行字段
        /// </summary>
        /// <param name="conduitid"></param>
        /// <param name="settab"></param>
        public static void setSqlUpdataField(int conduitid, DataTable settab)
        {
            bool flag = true;
            string sql = null;
            string debug = null;
            int n=0;
            DataTable dt = new DataTable();

            try
            {
                foreach (DataRow dr in settab.Rows)
                {

                    if (!string.IsNullOrEmpty(dt.Rows[0]["sqlfield"].ToString()))
                    {
                        settab.CaseSensitive = false;

                        string[] p = dt.Rows[0]["sqlparam"].ToString().Split(',');
                        object[] param = new object[p.Length + 1];
                        param[0] = conduitid;
                        for (int i = 0; i < p.Length; i++)
                            param[i + 1] = p[i];

                        sql = string.Format(dt.Rows[0]["sqlfield"].ToString(), param);



                        debug = JsonConvert.SerializeObject(settab, new DataTableConverter());
                        dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                        if (dt.Rows.Count > 0)
                            dr["valuedata"] = dt.Rows[0][p[n]].ToString();
                        
                        ++n;
                    }
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "==========setSqlUpdataField(int conduitid, DataTable settab)获取数据时异常==========>\r\n"

                                                                  + "sqlstr:[" + sql + "]\r\n"
                                                                  + "conduitid:[" + conduitid + "]\r\n"
                                                                  + "settab:" + debug + "\r\n"
                                                                  + "[" + e.ToString() + "]\r\n"
                                                 + "===========================================END==============================================\r\n");
            }
         
        }

        /// <summary>
        /// 获取订单
        /// </summary>
        /// <param name="order"></param>
        /// <param name="conduitid"></param>
        /// <returns></returns>
        public static string setSqlUpdataField(string order,int conduitid)
        {
            string sql = "select top 1 extend1 from public_order_2017 where conduitid="+conduitid+" and userOrder='"+order+"'  and code is null";

            string info = string.Empty;
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = dt.Rows[0]["extend1"].ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>setSqlUpdataField获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        /*public static bool setSysField(string sql, object[] param, DataTable settab)
        {
            bool flag = true;
            DataTable dt=new DataTable();
            try
            {
                if(param.Length>0)
                    dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, string.Format(sql, param)).Tables[0];

                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow dr in settab.Rows)
                    {
                         string name=dr["fieldname"].ToString();
                         DataRow[] value = dt.Select(name);//查询
                         if (value.Length>0)
                           dr["value"] = value[0][name].ToString();
                    }
                }
                
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>getSysField获取数据时异常+[" + e.ToString() + "]\r\n");
                return flag;
            }

            return flag;
        
        }*/


        public static bool getUnsubscribe(string code)
        {
            string sql = "select top 1 infoid from action where unsubscribe=(SELECT max(unsubscribe) FROM action where unsubscribe=left('" + code + "',LEN(unsubscribe))) and unsubscribe<>'' ";

            bool flag=false;
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (old!=null)
                    flag = true;
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getUnsubscribe获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;

        }

        public static bool setSysField(int fee, int companyid, int conduitid, DataTable settab)
        {
            string sql = null;
            string mobile = null;
            bool flag = true;
            DataTable dt = new DataTable();
            try
            {
                sql = "select top 1 a.productid,a.ordered as message,b.address as senderAddress,a.buyid,a.infoid as actionid,REPLACE(REPLACE(REPLACE(REPLACE(convert(varchar(50), sysdatetime()),'-',''),':',''),' ',''),'.','') as transactionId"
                    + ",companyid,conduitid from action as a left join product as b on b.infoid=a.productid"
                    + " where companyid=" + companyid + " and price=" + fee + " and a.conduitid=" + conduitid + " and syncstart=1";
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                {
                    DataRow[] mrow = settab.Select("fieldname='mobile'");//查询
                    if (mrow.Length > 0)
                        mobile = mrow[0]["value"].ToString();
                    foreach (DataRow dr in settab.Rows)
                    {
                        string name = dr["fieldname"].ToString();
                        for (int i = 0; i < dt.Columns.Count; i++)
                        {
                            string value = dt.Columns[i].ColumnName;
                            if (name.ToLower() == value.ToLower())
                                dr["value"] = dt.Rows[0][value].ToString();
                        }
                        setFieldValue(dr, mobile);
                    }
                }
                else
                    flag = false;
                    
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>setSysField(int fee, int companyid, int conduitid, DataTable settab)获取数据异常[" + e.ToString() + "]");
                return false;
            }

            return flag;
        }



        public static bool setSysField(string code, int conduitid, DataTable settab)//在配置时可自动设置conduitid不必再进行赋值操作
        {
            bool flag = true;
            DataTable dt = new DataTable();
            string mobile = null;
            string debug = null;
            try
            {
                debug = JsonConvert.SerializeObject(settab, new DataTableConverter());
                if (null != code)
                {
                    string sql = "select top 1 a.buyid,a.companyID,a.conduitID,a.productCode,a.buyname,a.productID,b.address as senderAddress from action as a left join product as b on b.infoid=a.productID where a.conduitid=" + conduitid +
" and  (ordered =(SELECT max(ordered) FROM action where ordered=left('" + code + "',LEN(ordered))) or unsubscribe='" + code + "')";
                    dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                }
                dt.CaseSensitive = false;

                DataRow[] mrow = settab.Select("fieldname='mobile'");//查询
                if (mrow.Length > 0)
                    mobile = mrow[0]["value"].ToString();
                foreach (DataRow dr in settab.Rows)
                {
                    string name = dr["fieldname"].ToString();
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        string value = dt.Columns[i].ColumnName;
                        if (name.ToLower() == value.ToLower())
                            dr["value"] = dt.Rows[0][value].ToString();
                    }
                    setFieldValue(dr, mobile);
                }

            }
            catch (Exception e)
            {

                LogHelper.WriteLog(typeof(Service), "==========setSysField(string code, int conduitid, DataTable settab)获取数据时异常==========>\r\n"
                                                                   + "code:[" + code + "]\r\n"
                                                                   + "conduitid:[" + conduitid + "]\r\n"
                                                                   + "settab:" + debug + "\r\n"
                                                                   + "[" + e.ToString() + "]\r\n"
                                                  + "===========================================END==============================================\r\n");

                return false;
            }
            return flag;
        }

        public static bool setSysFieldx(string code, int conduitid, DataTable settab)//在配置时可自动设置conduitid不必再进行赋值操作
        {
            bool flag = true;
            DataTable dt = new DataTable();
            string mobile = null;
            string debug = null;
            try
            {
                debug = JsonConvert.SerializeObject(settab, new DataTableConverter());
                if (null != code)
                {
                    string sql = "select top 1 a.buyid,a.companyID,a.conduitID,a.productCode,a.buyname,a.productID,b.address as senderAddress,a.price as fee from action as a left join product as b on b.infoid=a.productID where a.conduitid=" + conduitid +
" and  (ordered =(SELECT max(ordered) FROM action where ordered=left('" + code + "',LEN(ordered))) or unsubscribe='" + code + "')";
                    dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                }
                dt.CaseSensitive = false;

                DataRow[] mrow = settab.Select("fieldname='mobile'");//查询
                if (mrow.Length > 0)
                    mobile = mrow[0]["value"].ToString();
                foreach (DataRow dr in settab.Rows)
                {
                    string name = dr["fieldname"].ToString();
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        string value = dt.Columns[i].ColumnName;
                        if (name.ToLower() == value.ToLower())
                            dr["value"] = dt.Rows[0][value].ToString();
                    }
                    setFieldValue(dr, mobile);
                }

            }
            catch (Exception e)
            {

                LogHelper.WriteLog(typeof(Service), "==========setSysField(string code, int conduitid, DataTable settab)获取数据时异常==========>\r\n"
                                                                   + "code:[" + code + "]\r\n"
                                                                   + "conduitid:[" + conduitid + "]\r\n"
                                                                   + "settab:" + debug + "\r\n"
                                                                   + "[" + e.ToString() + "]\r\n"
                                                  + "===========================================END==============================================\r\n");

                return false;
            }
            return flag;
        }


        public static bool setSysField(string code, int conduitid, int companyid, DataTable settab)//在配置时可自动设置conduitid不必再进行赋值操作
        {
            bool flag = true;
            DataTable dt = new DataTable();
            string mobile = null;
            try
            {

                if (null != code)
                {
                    string sql = "select top 1 buyid,companyID,conduitID,productCode,buyname,productID from action where conduitid=" + conduitid + " and companyid="+companyid+" and (ordered =(SELECT max(ordered) FROM action where ordered=left('" + code + "',LEN(ordered))) or unsubscribe='" + code + "')";
                    dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                }
                dt.CaseSensitive = false;

                DataRow[] mrow = settab.Select("fieldname='mobile'");//查询
                if (mrow.Length > 0)
                    mobile = mrow[0]["value"].ToString();
                foreach (DataRow dr in settab.Rows)
                {
                    string name = dr["fieldname"].ToString();
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        string value = dt.Columns[i].ColumnName;
                        if (name.ToLower() == value.ToLower())
                            dr["value"] = dt.Rows[0][value].ToString();
                    }
                    setFieldValue(dr, mobile);
                }

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setSysField(string code, int conduitid, int companyid, DataTable settab)获取数据时异常+[" + e.ToString() + "]\r\n");
                return false;
            }
            return flag;
        }



        public static bool setSysField(string sqlstr, string sqlparam, int conduitid, DataTable settab)
        {
            bool flag = true;
            string sql = null;
            string mobile = null;
            string debug = null;

            DataTable dt = new DataTable();
            if (!string.IsNullOrEmpty(sqlparam))
            {
                settab.CaseSensitive = false;

                string[] p = sqlparam.Split(',');
                object[] param = new object[p.Length + 1];
                param[0] = conduitid;
                for (int i = 0; i < p.Length; i++)
                {
                    DataRow[] item = settab.Select("paramname='" + p[i] + "'");//查询
                    if (item.Length > 0)
                        param[i + 1] = item[0]["value"].ToString();
                }
                sql = string.Format(sqlstr, param);
            }
            else
                sql = string.Format(sqlstr, conduitid);
            
            try
            {
                 debug = JsonConvert.SerializeObject(settab, new DataTableConverter());
                 dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                 DataRow[] mrow = settab.Select("fieldname='mobile'");//查询
                 if (mrow.Length > 0)
                     mobile = mrow[0]["value"].ToString();
                 foreach (DataRow dr in settab.Rows)
                 {
                     string name = dr["fieldname"].ToString();
                     for (int i = 0; i < dt.Columns.Count; i++)
                     {
                         string value = dt.Columns[i].ColumnName;
                         if (name.ToLower() == value.ToLower())
                             dr["value"] = dt.Rows[0][value].ToString();
                     }
                     setFieldValue(dr, mobile);
                 }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "==========setSysField(string sqlstr, string sqlparam, int conduitid, DataTable settab)获取数据时异常==========>\r\n"
                                                                  + "sqlparam:[" + sqlparam + "]\r\n"
                                                                  + "sqlstr:[" + sqlstr + "]\r\n"
                                                                  + "conduitid:[" + conduitid + "]\r\n"
                                                                  + "settab:" + debug + "\r\n"
                                                                  + "[" + e.ToString() + "]\r\n"
                                                 + "===========================================END==============================================\r\n");
                return false;
            }

            return flag;
        }


        public static bool setSysField(string sqlstr, string sqlparam, int conduitid, DataTable settab, int format)
        {
            bool flag = true;
            string sql = null;
            string mobile = null;
            string debug = null;

            DataTable dt = new DataTable();
            if (!string.IsNullOrEmpty(sqlparam))
            {
                settab.CaseSensitive = false;

                string[] p = sqlparam.Split(',');
                object[] param = new object[p.Length + 1];
                param[0] = conduitid;
                for (int i = 0; i < p.Length; i++)
                {
                    DataRow[] item = settab.Select("paramname='" + p[i] + "'");//查询
                    if (item.Length > 0)
                        param[i + 1] = item[0]["value"].ToString();
                }
                sql = string.Format(sqlstr, param);
            }
            else
                sql = string.Format(sqlstr, conduitid);

            try
            {
                debug = JsonConvert.SerializeObject(settab, new DataTableConverter());
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                DataRow[] mrow = settab.Select("fieldname='mobile'");//查询
                if (mrow.Length > 0)
                    mobile = mrow[0]["value"].ToString();
                foreach (DataRow dr in settab.Rows)
                {
                    string name = dr["fieldname"].ToString();
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        string value = dt.Columns[i].ColumnName;
                        if (name.ToLower() == value.ToLower())
                            dr["value"] = dt.Rows[0][value].ToString();
                    }
                    setFieldValue(dr, mobile, format);
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "==========setSysField(string sqlstr, string sqlparam, int conduitid, DataTable settab)获取数据时异常==========>\r\n"
                                                                  + "sqlparam:[" + sqlparam + "]\r\n"
                                                                  + "sqlstr:[" + sqlstr + "]\r\n"
                                                                  + "conduitid:[" + conduitid + "]\r\n"
                                                                  + "settab:" + debug + "\r\n"
                                                                  + "[" + e.ToString() + "]\r\n"
                                                 + "===========================================END==============================================\r\n");
                return false;
            }

            return flag;
        }

        public static string setFieldValue(DataRow dr,string mobile)
        {
            string name = dr["fieldname"].ToString();
            string value = null;
            try
            {
                if (name.ToLower() == "correlator")
                    dr["value"] = GUID.CreatGUID("N").ToUpper();
                else if (name.ToLower() == "streamingno")
                {
                    if (string.IsNullOrEmpty(dr["value"].ToString()))
                        dr["value"] = StreamingNo();
                }
                else if (name.ToLower() == "area" && dr["area"].ToString() == "1")
                {
                    
                    mobileArea area = getMobileArea(mobile);
                    dr["value"] = area.Area;
                    
                }
                else if (name.ToLower() == "fee" && dr["fee"].ToString() == "1")
                {
                    string fee = dr["value"].ToString();
                    dr["value"] = fee.Substring(0, fee.Length - 2);
                }

                if (!string.IsNullOrEmpty(dr["converted"].ToString()))
                {
                    JObject json = JObject.Parse(dr["converted"].ToString());
                    if (null != json[dr["value"].ToString()])
                        dr["value"] = json[dr["value"].ToString()].ToString();
                    else if (null != json["other"])
                        dr["value"] = json["other"].ToString();

                    /*IEnumerable<JProperty> properties = json.Properties();
                    foreach (JProperty item in properties)
                    {
                        if(item.Name==dr["value"].ToString())
                           dr["value"] = json[dr["value"].ToString()].ToString();

                    }*/
                }
                if (!string.IsNullOrEmpty(dr["defvalue"].ToString()))
                    dr["value"] = dr["defvalue"].ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setFieldValue获取数据时异常+[" + e.ToString() + "]\r\n");
                return value;
            }
            return value;
        }


        public static string setFieldValue(DataRow dr, string mobile, int format)
        {
            string name = dr["fieldname"].ToString();
            string value = null;
            try
            {
                if (name.ToLower() == "correlator")
                    dr["value"] = GUID.CreatGUID("N").ToUpper();
                else if (name.ToLower() == "streamingno")
                {
                    if (string.IsNullOrEmpty(dr["value"].ToString()))
                        dr["value"] = StreamingNo();
                }
                else if (name.ToLower() == "area" && dr["area"].ToString() == "1")
                {
                      mobileArea area = getMobileArea(mobile);
                      dr["value"] = area.Area;
                }
                else if (name.ToLower() == "fee" && dr["fee"].ToString() == "1")
                {
                    string fee = dr["value"].ToString();
                    dr["value"] = fee.Substring(0, fee.Length - 2);
                }

                if (!string.IsNullOrEmpty(dr["converted"].ToString()) && format>0)
                {
                   
                    JObject json = JObject.Parse(dr["converted"].ToString());
                    if (null != json[dr["value"].ToString()])
                        dr["value"] = json[dr["value"].ToString()].ToString();
                    else if (null != json["other"])
                        dr["value"] = json["other"].ToString();

                    /*IEnumerable<JProperty> properties = json.Properties();
                    foreach (JProperty item in properties)
                    {
                        if(item.Name==dr["value"].ToString())
                           dr["value"] = json[dr["value"].ToString()].ToString();

                    }*/
                }
                if (!string.IsNullOrEmpty(dr["defvalue"].ToString()))
                    dr["value"] = dr["defvalue"].ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setFieldValue获取数据时异常+[" + e.ToString() + "]\r\n");
                return value;
            }
            return value;
        }

        //同步接口的返回值在配置中实现，目前暂时使用固定模式
        public static void syncNotify(string tabname, DataTable dt, DataTable config)
        {
            int olid = 0;
            string method = "get";
            string param = null;
            string result = null;
            int flag = -1;
            string uptab = null;
            JObject data = null;
            string ainfo = null;
            JObject action = new JObject();
            try
            {
                //string dinfo = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                data = getSyncData(dt);//不必使用该方法，可直接使用DataTable 的select 进行查询判断
                if (null != data)
                {

                    ainfo = getActionInfo(data["message"].ToString(), Convert.ToInt32(data["conduitID"]));

                    if (string.IsNullOrEmpty(ainfo))
                        return;
                    action = JObject.Parse(ainfo);

                   
                    if (action["syncFlag"].ToString() == "0")//同步开关
                        return;

                    if (data["OPType"].ToString() == "1")
                    { 
                        if (action["optypeid"].ToString() == "0")//同步退订是否打开
                            return;

                        int sync = searchSyncData(data["mobile"].ToString(), Convert.ToInt32(action["conduitID"].ToString()), Convert.ToInt32(action["companyID"].ToString()));
                        if (sync == 0)//如果未同步订购则返回
                            return;

                        /*int pid = search72hours(data["mobile"].ToString());//查询订购时间是否在72小时内
                        if (pid == 0)
                           return;*/

                        DataRow[] message = dt.Select("fieldname='message'");
                        message[0]["value"] = action["unsubscribe"].ToString();
                        data["message"] = action["unsubscribe"].ToString();
                        flag = 0;
                    }
                    else if (data["OPType"].ToString() == "0")
                    {
                        int point = Convert.ToInt32(action["point"]);
                        int companyID = Convert.ToInt32(action["companyID"]);
                        int conduitID = Convert.ToInt32(action["conduitID"]);

                       if (action["buyID"].ToString() == "2")
                        {
                            int infoid = searchRecord(data["mobile"].ToString(), conduitID);    //searchRecord(data["mobile"].ToString());
                            //if (infoid == 0)
                              //  addRecordInfo(data["StreamingNo"].ToString(), data["correlator"].ToString(), data["mobile"].ToString(), data["area"].ToString(),conduitID);
                                //addRecordInfo(data["StreamingNo"].ToString(), data["correlator"].ToString(), data["mobile"].ToString(), data["area"].ToString());
                            if (infoid>0)
                                return;
                        }

                        flag = setDeductPoint(conduitID, companyID, point, action["ordered"].ToString(), action["buyID"].ToString());

                    }
                    else
                        return;


                    string url = action["syncUrl"].ToString();
                    if (string.IsNullOrEmpty(url))
                    {
                        LogHelper.WriteLog(typeof(Service), "============>SyncDatax[同步地址错误！]");
                        return;
                    }
                    
                    
                    if (flag == 0 && data["resultCode"].ToString() == "0")
                    {
                        uptab = config.Rows[0]["tablename"].ToString();

                        param = "StreamingNo=" + data["StreamingNo"].ToString() + "&productCode=" + data["productCode"].ToString() + "&Mobile=" + data["mobile"].ToString() + "&MobileType=" + action["operators"].ToString() + "&Fee=" + data["fee"].ToString() + "&Message=" + Utils.ConvertUtfUrlPram(data["message"].ToString()) + "&OPType=" + data["OPType"].ToString() + "&senderAddress=" + action["address"].ToString() + "&resultCode=" + data["resultCode"].ToString();

                        DataTable field = getField("10", "4");//同步数据目前固定使用固定字段查询并插入 后续将改为设置字段
                        //getField(config.Rows[0]["infoid"].ToString(),config.Rows[0]["conduitid"].ToString());


                        olid = AddSyncData(field, tabname, config.Rows[0]["tablename"].ToString(), data["correlator"].ToString(), "syncpack", url + "?" + param);
                        if (olid > 0)
                        {
                            if (!string.IsNullOrEmpty(action["syncMethod"].ToString()))
                                method = action["syncMethod"].ToString();

                            result = Utils.GetService(method, url, param);
                           
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

        /// <summary>
        /// 查询是否已经同步
        /// </summary>
        /// <param name="correlator"></param>
        /// <returns></returns>
      public static int searchSyncData(string mobile,int conduitID, int companyID)
        {
            string sql = "select top 1 infoid from public_sync_2017 where datatime between dateadd(day,-30,GETDATE()) and GETDATE() and mobile='" + mobile + "' and conduitID=" + conduitID + " and companyid=" + companyID + " and buyid=2 and optype=0";
            int info = 0;

            try 
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchSyncData(string mobile,int conduitID, int companyID)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


      /// <summary>
      /// 查询订购和退订是否都已同步
      /// </summary>
      /// <param name="mobile"></param>
      /// <param name="conduitID"></param>
      /// <param name="companyID"></param>
      /// <param name="tablename"></param>
      /// <returns></returns>
      public static int searchSyncData(string mobile, int conduitID, int companyID, string tablename)
      {
          string sql = "select count(infoid) from " + tablename + " where mobile='" + mobile + "' and conduitID=" + conduitID + " and companyid=" + companyID + " and buyid=2  and optype in(0,1)";
          int info = 0;

          try
          {
              object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
              if (DBNull.Value != old)
                  info = Convert.ToInt32(old);
          }
          catch (Exception e)
          {
              LogHelper.WriteLog(typeof(Service), sql + "============>searchSyncData获取数据异常[" + e.ToString() + "]");
              return info;
          }

          return info;
      }

        /// <summary>
        /// 检索包月历史号码
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        static int searchRecord(string mobile)
        {
            string sql = "select top 1 infoid from public_record where mobile='" + mobile + "'";
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecord获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }



        public static int searchRecord(string mobile, int conduitID)
        {
            string sql = "select top 1 infoid from public_notify_2017 where datatime between dateadd(month,-3,GETDATE()) and GETDATE() and mobile='" + mobile + "' and conduitID=" + conduitID;
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecord(string mobile, int conduitID)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        /// <summary>
        /// 查询业务号码是否存在重复订购
        /// </summary>
        /// <param name="mobile"></param>
        /// <param name="conduitID"></param>
        /// <param name="tablename">可选同步表或订购历史表</param>
        /// <returns></returns>
        public static int searchRecord(string mobile, int conduitID, string tablename)
        {
            string sql = "select top 1 infoid from " + tablename + " where mobile='" + mobile + "' and conduitID=" + conduitID + " and buyid=2 and optype=0";
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecord(string mobile, int conduitID, string tablename)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static int searchRecordx(string mobile, int conduitID, string tablename)
        {
            string sql = "select top 1 infoid from " + tablename + " where datatime>dateadd(month,-3,GETDATE()) and mobile='" + mobile + "' and conduitID=" + conduitID + " and optype=0";
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecord(string mobile, int conduitID, string tablename)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static int searchRecordxx(string mobile, int conduitID, string tablename)
        {
            string sql = "select top 1 infoid from " + tablename + " where datatime>dateadd(month,-3,GETDATE()) and mobile='" + mobile + "' and conduitID=" + conduitID + "  and optype=0 and resultCode=0";
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecordxx(string mobile, int conduitID, string tablename)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static int searchRecordx(string mobile, int conduitID, int productID, string tablename)
        {
            string sql = "select top 1 infoid from " + tablename + " where datatime between dateadd(month,-3,GETDATE()) and GETDATE() and mobile='" + mobile + "' and conduitID=" + conduitID + "  and optype=0  and  productID=" + productID;
            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecordx(string mobile, int conduitID, int productID, string tablename)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }

        public static int searchRecordx(string field, int conduitID, int productID, string tablename,string value)
        {

            string sql = string.Format("select top 1 infoid from {0} where datatime>dateadd(month,-3,GETDATE()) and {1}='{2}' and conduitID={3} and optype=0", tablename, field, field, conduitID);

            if (productID > 0)
                sql = string.Format("select top 1 infoid from {0} where datatime>dateadd(month,-3,GETDATE()) and {1}='{2}' and conduitID={3} and optype=0 and productID={4}", tablename, field, field, conduitID, productID);

            int info = 0;

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecordx(string field, int conduitID, int productID, string tablename,string value)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }



        /// <summary>
        /// 统计号码请求次数
        /// </summary>
        /// <param name="field"></param>
        /// <param name="conduitID"></param>
        /// <param name="productID"></param>
        /// <param name="companyID"></param>
        /// <param name="tablename"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string searchRecordCount(string field, int conduitID, int productID, int companyID, string tablename, string value)
        {
            string info =null;
            string param="";
            Time time = new Time();
            string date = time.GetMonth();

            string msql = string.Format("select count(infoid) from {0} where datatime between " + date + " and {1}='{2}' and conduitID={3} and codeflag=0", tablename, field, value, conduitID);
            
            if (productID > 0)
                param += " and productID=" + productID;

            if (companyID > 0)
                param += " and companyID=" + companyID;

            string sql = string.Format("select count(infoid) as day,(" + msql + param + ") as month from {0} where datatime>=convert(char(10),GETDATE(),120) and {1}='{2}' and conduitID={3} and codeflag=0", tablename, field, value, conduitID);
            try
            {

                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql + param).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecordCount(string field, int conduitID, int productID, string tablename,string value)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        /// <summary>
        /// 统计号码费金额
        /// </summary>
        /// <param name="field">查询字段</param>
        /// <param name="conduitID"></param>
        /// <param name="productID"></param>
        /// <param name="companyID"></param>
        /// <param name="tablename">表</param>
        /// <param name="value">查询内容</param>
        /// <returns></returns>
        public static string searchRecordFee(string field, int conduitID, int productID, int companyID,string tablename, string value)
        {
            string info = null;
            string param = "";
            Time time = new Time();
            string date = time.GetMonth();
            string msql = string.Format("select sum(fee) from {0} where datatime>=convert(char(10),GETDATE(),120) and {1}='{2}'and conduitid={3} and optype=0 and resultCode=0", tablename, field, value, conduitID);

            if (productID > 0)
                param += " and productID=" + productID;

            if (companyID > 0)
                param += " and companyID=" + companyID;

            string sql = string.Format("select sum(fee) as dayfee,(" + msql + param + ") as monthfee from {0} as a where datatime between " + date + " and {1}='{2}' and conduitID={3} and optype=0 and resultCode=0", tablename, field, value, conduitID);

            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql + param).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>searchRecordFee(string field, int conduitID, int productID, string tablename,string value)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        /// <summary>
        /// 总量或单省日月限统计
        /// </summary>
        /// <param name="conduitid"></param>
        /// <param name="productid"></param>
        /// <param name="companyid"></param>
        /// <param name="productCode"></param>//后期使用 actionid
        /// <param name="area"></param>
        /// <param name="countfield"></param>
        /// <param name="distinct">是否滤重</param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        /*public static string getLimitTotal(int conduitid, int productid, int companyid, string productCode, string area, int fee, string countfield, bool distinct, string tablename)
        {
            string info = null;

            string paramstr = string.Empty;
            string countitem = string.Empty;

            Time time = new Time();
            string date = time.GetMonth();

            if (!string.IsNullOrEmpty(countfield))
                countitem = "count(" + countfield + ")";
            else
                countitem = "count(userOrder)";

            if (distinct)
                countitem = countitem.Replace("count(", "count(DISTINCT(").Replace(")", "))");

             if (!string.IsNullOrEmpty(productCode))
                paramstr += " and productCode='"+ productCode+"'";

            if (productid > 0)
                paramstr += " and productid=" + productid;

            if (!string.IsNullOrEmpty(area))
                paramstr += " and area like '" + area + "%'";

            if (companyid > 0)
                paramstr += " and companyid='" + companyid + "'";

            if (fee > 0)
                paramstr += " and fee='" + fee + "'";

            if(tablename=="public_order_2017")
                paramstr += " and codeflag=1";

            string sql = "SELECT " + countitem + " FROM " + tablename + " as a WITH(NOLOCK) where datatime>=convert(char(10),GETDATE(),120) and resultCode=0 and OPType=0 and conduitid=" + conduitid + paramstr;

            sql = string.Format("select " + countitem + " as month,(" + sql + ") as day from {0} as a where datatime between {1} and conduitid={2} and optype=0 and resultCode=0{3}", tablename, date, conduitid, paramstr);

            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {

                return info;
            }
            return info;
        }*/



        public static DataTable getLimitTotal(int conduitid, int productid, int companyid, string productCode, string area, int fee, string code, string countfield, bool distinct, string tablename)
        {
            DataTable dt = new DataTable();

            string paramstr = string.Empty;
            string countitem = string.Empty;

            Time time = new Time();
            string date = time.GetMonth();

            
            if (!string.IsNullOrEmpty(countfield))
                countitem = "count(" + countfield + ") as number";

            if (distinct)
                countitem = countitem.Replace("count(", "count(DISTINCT(").Replace(")", "))");

            if (!string.IsNullOrEmpty(productCode))
                paramstr += " and productCode='" + productCode + "'";

            if (productid > 0)
                paramstr += " and productid=" + productid;

            if (!string.IsNullOrEmpty(area))
                paramstr += " and area like '" + area + "%'";

            if (companyid > 0)
                paramstr += " and companyid='" + companyid + "'";

            if (fee > 0)
                paramstr += " and fee='" + fee + "'";

            if (!string.IsNullOrEmpty(code))
                paramstr += " and codeflag=1 and code='"+code+"'";
            else
                paramstr += " and codeflag=0";

            string sql = "SELECT " + countitem + " FROM " + tablename + " as a WITH(NOLOCK) where datatime>=convert(char(10),GETDATE(),120) and resultCode=0 and OPType=0 and conduitid=" + conduitid + paramstr;

            if (!string.IsNullOrEmpty(code))
             sql = string.Format("select " + countitem + " as month,(" + sql + ") as day from {0} as a where datatime between {1} and conduitid={2} and optype=0 and resultCode=0{3}", tablename, date, conduitid, paramstr);

            try
            {
                 dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                 
            }
            catch (Exception e)
            {

                return dt;
            }
            return dt;
        }


        /// <summary>
        /// 添加包月卡号历史记录
        /// </summary>
        /// <param name="code"></param>
        public static void addRecordInfo(string StreamingNo, string correlator, string mobile, string area)
        {
            int olid = 0;
            string sql = "INSERT INTO public_record(StreamingNo,correlator,mobile,area) values('" + StreamingNo + "','" + correlator + "','" + mobile + "','" + area + "')";

            try
            {
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), sql + "============>addRecordInfo添加数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>addRecordInfo[" + e.ToString() + "]");

            }
        }


        public static void addRecordInfo(string StreamingNo, string correlator, string mobile, string area, int conduitID)
        {
            int olid = 0;
            string sql = "INSERT INTO public_record(StreamingNo,correlator,mobile,area,conduitID) values('" + StreamingNo + "','" + correlator + "','" + mobile + "','" + area + "'," + conduitID + ")";

            try
            {
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                if (olid < 1)
                    LogHelper.WriteLog(typeof(Service), sql + "============>addRecordInfo添加数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>addRecordInfo[" + e.ToString() + "]");

            }
        }

        //该方法后续需要更改 因为订购和通知数据可能存在不同的表中
        public static int search72hours(string mobile)
        {
            int info = 0;
            string sql = "select top 1 case when CONVERT(varchar(30),GETDATE(),120)<CONVERT(varchar(30),DATEADD(hh,+72,datatime),120) then infoid else 0 end from public_notify_2017 where mobile='" + mobile + "' and buyid=2 and optype=0";
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (null != old)
                info = Convert.ToInt32(old);

            return info;
        }


        public static int search72hours(string mobile,string imsi,string tablename)
        {
            int info = 0;
            string param=string.Empty;
            if (!string.IsNullOrEmpty(mobile))
                param = " and mobile='" + mobile + "'";

            if (!string.IsNullOrEmpty(param))
                param += " and imsi='" + imsi + "'";

            string sql = "select top 1 case when CONVERT(varchar(30),GETDATE(),120)<CONVERT(varchar(30),DATEADD(hh,+72,datatime),120) then infoid else 0 end from public_notify_2017 where buyid=2 and optype=0" + param;
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (null != old)
                info = Convert.ToInt32(old);

            return info;
        }



        public static int search72hours(DataTable data,string sqlinfo,string param)
        {
            int info = 0;
            try
            {
                if (string.IsNullOrEmpty(sqlinfo) || string.IsNullOrEmpty(param))
                    return -1;

                DataRow[] hours=data.Select("fieldname='hours72'");
                if (hours.Length == 0)
                    return info;
                param = "conduitID," + param;
                string[] items = param.Split(',');
                object[] pm = new object[items.Length];
               
                for (int i = 0; i < items.Length; i++)
                {
                    DataRow[] dr = data.Select("fieldname='" + items[i].ToString() + "'");
                    if (dr.Length > 0)
                        pm[i] = dr[0]["value"].ToString();
                }
                string sql = string.Format(sqlinfo, pm);

                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (null != old)
                {
                    hours[0]["value"] = Convert.ToInt32(old);
                    info = Convert.ToInt32(old);
                }
            }
            catch (Exception e)
            {
                return -1;

            }
            return info;
        }


        /// <summary>
        /// 获取同步接口字段
        /// </summary>
        /// <param name="configid"></param>
        /// <returns></returns>
        public static DataTable getSyncInterfaceField(int configid)
        {
            string sql = null;
            string field = string.Empty;
            DataTable dt = new DataTable();
            try
            {
                sql = "select * from public_interface where configid=" + configid;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getSyncInterfaceField获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        /// <summary>
        /// 获取同步接口字段
        /// </summary>
        /// <param name="configid"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static DataTable getSyncInterfaceField(int configid,string value)
        {
            string sql = null;
            string field = string.Empty;
            DataTable dt = new DataTable();
            try
            {
                sql = string.Format("select * from public_interface where configid={0} and setValue='{1}'",configid,value);
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getSyncInterfaceField(int configid,string value)获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }



        /// <summary>
        /// 获取上行接口字段
        /// </summary>
        /// <param name="configid"></param>
        /// <returns></returns>
        public static DataTable getUpInterfaceField(int configid)
        {
            string sql = null;
            string field = string.Empty;
            DataTable dt = new DataTable();
            try
            {
                sql = "select * from public_interface where configid=" + configid;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getUpInterfaceField获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        /// <summary>
        /// 获同步取接口参数数据
        /// </summary>
        /// <param name="id"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static DataTable getSyncInterfaceData(string id, string field)
        {
            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql = "select top 1 " + field + " from public_sync_2017 where correlator='" + id +"'";
                 dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getSyncInterfaceData获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        /// <summary>
        /// 获同步取接口参数数据
        /// </summary>
        /// <param name="id"></param>
        /// <param name="field"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static DataTable getSyncInterfaceData(string id, string field, string table)
        {
            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql =string.Format("select top 1 {0} from {1} where correlator='{2}'",field,table,id);
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getSyncInterfaceData(string id, string field, string table)获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        /// <summary>
        /// 获同步取接口参数数据
        /// </summary>
        /// <param name="data"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static bool getSyncInterfaceData(DataTable data, DataTable field)
        {
            string sql = null;
            bool falg = true;
            try
            {
                field.Columns.Add("value", typeof(string));
                data.CaseSensitive = false;
                foreach (DataRow dr in field.Rows)
                {
                    if (dr["isRequest"].ToString() == "1")
                    {
                        DataRow[] value = data.Select("paramname='" + dr["inField"].ToString() + "'");
                        if (value.Length > 0)
                            dr["value"] = value[0]["value"].ToString();
                        else
                            dr["value"] = string.Empty;
                    }
                    else
                    {
                        DataRow[] value = data.Select("fieldname='" + dr["inField"].ToString() + "'");
                        if (value.Length > 0)
                            dr["value"] = value[0]["value"].ToString();
                        else
                            dr["value"] = dr["inField"].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getSyncInterfaceData获取数据异常[" + e.ToString() + "]");
                return false;
            }

            return falg;
        }




        /// <summary>
        /// 获取上行接口参数数据
        /// </summary>
        /// <param name="data"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static bool getUpInterfaceData(Hashtable data, DataTable field)
        {
            
            bool falg = true;
            try
            {

                field.CaseSensitive = false;
                foreach (DataRow dr in field.Rows)
                {
                    if (!string.IsNullOrEmpty(dr["inField"].ToString()))
                        dr["valuedata"] = data[dr["inField"].ToString()].ToString();
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>getUpInterfaceData获取数据异常[" + e.ToString() + "]");
                return false;
            }

            return falg;
        }



        public static bool getUpInterfaceDatax(Hashtable data, DataTable field)
        {

            bool falg = true;
            try
            {

                field.CaseSensitive = false;
                foreach (DataRow dr in field.Rows)
                {
                    if (!string.IsNullOrEmpty(dr["inField"].ToString()) && null != data[dr["inField"].ToString()])
                        dr["valuedata"] = data[dr["inField"].ToString()].ToString();
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>getUpInterfaceData获取数据异常[" + e.ToString() + "]");
                return false;
            }

            return falg;
        }


        public static bool getUpInterfaceData(Hashtable data, DataTable settab, string sqlstr, string sqlparam, int conduitid)
        {
            bool flag = false;
            flag = getUpInterfaceDatax(data, settab);
            if(!string.IsNullOrEmpty(sqlstr) && !string.IsNullOrEmpty(sqlparam))
            {
                if(flag)
                    return setUpInterfaceData(sqlstr,sqlparam,conduitid,settab);
            }

            return flag;
        }


        /// <summary>
        /// 设置要加密的字段
        /// </summary>
        /// <param name="field"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string sortMd5Value(DataTable field)
        {
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;
                field.DefaultView.Sort = "outField ASC";
                field = field.DefaultView.ToTable();
                DataRow[] items = field.Select("md5=1");

                for (int i = 0; i < items.Length; i++)
                {
                    value += items[i]["outField"].ToString() + items[i]["valuedata"].ToString();

                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>sortMd5Value(DataTable field)获取数据异常[" + e.ToString() + "]");
                return null;
            }

            return value;
        }



        /// <summary>
        /// 设置要加密
        /// </summary>
        /// <param name="field"></param>
        /// <param name="setfield"></param>
        /// <param name="key"></param>
        /// <param name="Encoder"></param>
        /// <param name="uppercase"></param>
        /// <param name="sign"></param>
        /// <returns></returns>
        public static bool setMd5Value(DataTable field, bool setfield, string key, string Encoder, int uppercase, bool sign)
        {
            bool flag = false;
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;
                //field.DefaultView.Sort = "outField ASC";
                //field = field.DefaultView.ToTable();

                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");

                if (setfield == true)
                {
                    if (sign == true)
                    {
                        for (int i = 0; i < items.Length; i++)
                            value +="&"+items[i]["outField"].ToString() +"="+ items[i]["valuedata"].ToString();
                    }
                    else
                    {
                        for (int i = 0; i < items.Length; i++)
                            value += items[i]["outField"].ToString() + items[i]["valuedata"].ToString();
                    }
                    if(value.IndexOf("&")>-1)
                        value=value.Substring(1);
                }
                else
                {
                    for (int i = 0; i < items.Length; i++)
                        value += items[i]["valuedata"].ToString();
                }


                if (!string.IsNullOrEmpty(Encoder))
                    value = Utils.MD5(value, System.Text.Encoding.UTF8);
                else
                    value = Utils.MD5(value, System.Text.Encoding.UTF8);

                if (uppercase == 1)
                    md5field[0]["valuedata"] = value.ToUpper();
                else
                    md5field[0]["valuedata"] = value;
                /*foreach (DataRow dr in field.Rows)
                {
                    if (Convert.ToInt32(dr["setmd5"]) == 1)
                    {
                        dr["valuedata"] = value;
                        break;
                    }
                }*/

                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase, bool Sign)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }

        public static bool setPayMd5Value(DataTable field, bool setfield, string key, string Encoder, int uppercase, bool sign)
        {
            bool flag = false;
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;
                //field.DefaultView.Sort = "outField ASC";
                //field = field.DefaultView.ToTable();

                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");

                if (setfield == true)
                {
                    if (sign == true)
                    {
                        for (int i = 0; i < items.Length; i++)
                            value += "&" + items[i]["outField"].ToString() + "=" + items[i]["valuedata"].ToString();
                    }
                    else
                    {
                        for (int i = 0; i < items.Length; i++)
                            value += items[i]["outField"].ToString() + items[i]["valuedata"].ToString();
                    }
                    if (value.IndexOf("&") > -1)
                        value = value.Substring(1);
                }
                else
                {
                    for (int i = 0; i < items.Length; i++)
                        value += items[i]["valuedata"].ToString();
                }


                if (!string.IsNullOrEmpty(Encoder))
                    value = Utils.MD5(value, System.Text.Encoding.UTF8);
                else
                    value = Utils.MD5(value, System.Text.Encoding.UTF8);

                if (uppercase == 1)
                    md5field[0]["valuedata"] = value.ToUpper();
                else
                    md5field[0]["valuedata"] = value;
                /*foreach (DataRow dr in field.Rows)
                {
                    if (Convert.ToInt32(dr["setmd5"]) == 1)
                    {
                        dr["valuedata"] = value;
                        break;
                    }
                }*/

                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setPayMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase, bool Sign)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }

        public static bool setPayMd5Value(DataTable field, string key, string Encoder, int uppercase, string format, object[] param)
        {
            bool flag = false;
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {
                
                field.CaseSensitive = false;
            
                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");
                
                for (int i = 0; i < items.Length; i++)
                {
                 // value += "&" + items[i]["outField"].ToString() + "=" + items[i]["value"].ToString();
                    int j = 0;
                    object[] pavalue = new object[param.Length];
                    while (j < param.Length)
                    {
                        pavalue[j] = items[i][param[j].ToString()].ToString();
                        ++j;
                    }
                    value += string.Format(format, pavalue);
                }
                    
                    
                if (value.IndexOf("&") > -1)
                    value = value.Substring(1);
                
                if (!string.IsNullOrEmpty(Encoder))
                    value = Utils.MD5(Utils.ConvertEncoding(value + key, Encoder), System.Text.Encoding.UTF8);
                else
                    value = Utils.MD5(value+key, System.Text.Encoding.UTF8);

                if (uppercase == 1)
                    md5field[0]["valuedata"] = value.ToUpper();
                else
                    md5field[0]["valuedata"] = value;
                

                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setPayMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase, bool Sign, string valuefield)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }


        public static bool setPayMd5Value(DataTable field, string key, string Encoder, int uppercase, string format, object[] param, string valuename)
        {
            bool flag = false;
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;

                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");

                for (int i = 0; i < items.Length; i++)
                {
                    // value += "&" + items[i]["outField"].ToString() + "=" + items[i]["value"].ToString();
                    int j = 0;
                    object[] pavalue = new object[param.Length];
                    while (j < param.Length)
                    {
                        pavalue[j] = items[i][param[j].ToString()].ToString();
                        ++j;
                    }
                    value += string.Format(format, pavalue);
                }


                if (value.IndexOf("&") > -1)
                    value = value.Substring(1);

                if (!string.IsNullOrEmpty(Encoder))
                    value = Utils.MD5(Utils.ConvertEncoding(value + key, Encoder), System.Text.Encoding.UTF8);
                else
                    value = Utils.MD5(value + key, System.Text.Encoding.UTF8);

                if (uppercase == 1)
                    md5field[0][valuename] = value.ToUpper();
                else
                    md5field[0][valuename] = value;


                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setPayMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase, bool Sign, string valuefield)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }


        public static bool setPayMd5Value(DataTable field, string key, string Encoder, int uppercase, string format, object[] param, string valuename, bool removefirst)
        {
            bool flag = false;
            string value = string.Empty;
            string codefield = "valuedata";
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;

                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");

                for (int i = 0; i < items.Length; i++)
                {
                    // value += "&" + items[i]["outField"].ToString() + "=" + items[i]["value"].ToString();
                    int j = 0;
                    object[] pavalue = new object[param.Length];
                    while (j < param.Length)
                    {
                        pavalue[j] = items[i][param[j].ToString()].ToString();
                        ++j;
                    }
                    value += string.Format(format, pavalue);
                }


                if (removefirst)
                    value = value.Substring(1);

                if (!string.IsNullOrEmpty(Encoder))
                    value = Utils.MD5(Utils.ConvertEncoding(value + key, Encoder), System.Text.Encoding.UTF8);
                else
                    value = Utils.MD5(value + key, System.Text.Encoding.UTF8);

                if (!string.IsNullOrEmpty(valuename))
                    codefield = valuename;

                if (uppercase == 1)
                    md5field[0][codefield] = value.ToUpper();
                else if(uppercase == 2)
                    md5field[0][codefield] = value.ToLower();
                else
                    md5field[0][codefield] = value;

                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setPayMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase, bool Sign, string valuefield,bool removefirst)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }

        
        /// <summary>
        /// 设置要加密
        /// </summary>
        /// <param name="field"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool setMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase)
        {
            bool flag = false;
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;
                //field.DefaultView.Sort = "outField ASC";
                //field = field.DefaultView.ToTable();

                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");

                if (setfield == true)
                {
                    for (int i = 0; i < items.Length; i++)
                        value += items[i]["outField"].ToString() + items[i]["valuedata"].ToString();
                }
                else
                {
                    for (int i = 0; i < items.Length; i++)
                        value += items[i]["valuedata"].ToString();
                }

                if (!string.IsNullOrEmpty(Encoder))
                     value = Utils.MD5(Utils.ConvertEncoding(value, Encoder));
                else
                     value = Utils.MD5(value);

                if (uppercase==1)
                    md5field[0]["valuedata"] = value.ToUpper();
                else
                    md5field[0]["valuedata"] = value;
                /*foreach (DataRow dr in field.Rows)
                {
                    if (Convert.ToInt32(dr["setmd5"]) == 1)
                    {
                        dr["valuedata"] = value;
                        break;
                    }
                }*/

                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setMd5Value(DataTable field,bool setfield, string key, string Encoder, int uppercase)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }


        public static bool setMd5Value(DataTable field, string key, string Encoder, int uppercase)
        {
            bool flag = false;
            string value = string.Empty;
            DataTable dt = new DataTable();
            try
            {

                field.CaseSensitive = false;
            
                if (string.IsNullOrEmpty(key))
                    return flag;

                DataRow[] md5field = field.Select("setmd5=1");
                if (md5field.Length == 0)
                    return flag;

                DataRow[] items = field.Select("md5>0", "md5 asc");


                for (int i = 0; i < items.Length; i++)
                {
                    if (items[i]["outField"].ToString() == md5field[0]["outField"].ToString())
                        continue;
                    if (string.IsNullOrEmpty(items[i]["valuedata"].ToString()))
                        value += items[i]["outField"].ToString();
                    else
                        value += items[i]["outField"].ToString() + items[i]["valuedata"].ToString();
                }

                if (!string.IsNullOrEmpty(Encoder))
                    value = Utils.MD5(Utils.ConvertEncoding(value, Encoder)+key);
                else
                    value = Utils.MD5(value + key);

                if (uppercase == 1)
                    md5field[0]["valuedata"] = value.ToUpper();
                else
                    md5field[0]["valuedata"] = value;
                /*foreach (DataRow dr in field.Rows)
                {
                    if (Convert.ToInt32(dr["setmd5"]) == 1)
                    {
                        dr["valuedata"] = value;
                        break;
                    }
                }*/

                flag = true;

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setMd5Value(DataTable field, string key, string Encoder, int uppercase)获取数据异常[" + e.ToString() + "]");
                return flag;
            }

            return flag;
        }


        /// <summary>
        /// 设置对外输出接口格式
        /// </summary>
        /// <param name="fromatid"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string setFormatInterfaceData(int fromatid, DataTable data)
        {
            string param =string.Empty;
            data.DefaultView.Sort = "sort ASC";
            data = data.DefaultView.ToTable();
            data.Columns.Remove("infoid");
            data.Columns.Remove("sort"); 
            if (fromatid == 2)
            {
                foreach (DataRow dr in data.Rows)
                    param += "&" + dr["outField"].ToString().Trim() + "=" + dr["value"].ToString().Trim();
            }
            else if (fromatid == 1)
            {

            }

            return param.Substring(1);
        }


        /// <summary>
        /// 设置对外输出接口格式
        /// </summary>
        /// <param name="fromatid">格式id</param>
        /// <param name="data">数据</param>
        /// <param name="fromatCode">格式编码</param>
        /// <returns></returns>
        public static string setFormatInterfaceData(int fromatid, DataTable data, string fromatCode)
        {
            string param = string.Empty;
            data.DefaultView.Sort = "sort ASC";
            data = data.DefaultView.ToTable();
            data.Columns.Remove("infoid");
            data.Columns.Remove("sort");
            if (fromatid == 2)
            {
                foreach (DataRow dr in data.Rows)
                    param += "&" + dr["outField"].ToString() + "=" + dr["value"].ToString();
                param=param.Substring(1);
            }
            else if (fromatid == 1||fromatid == 0)
            {
                /*JObject jsonObj = JObject.Parse(fromatCode);
                IEnumerable<JProperty> properties = jsonObj.Properties();
                foreach (JProperty item in properties)
                {
                     string names = item.Name.ToString();
                     DataRow[] result = data.Select("outField='" + names + "'");
                     if(result.Length>0)
                         item.Value = result[0]["value"].ToString();
                }
                param = fromatCode;*/
                for(int i=0;i<data.Rows.Count;i++)
                {

                    param = FindJsonNodex(param, data.Rows[i]["outField"].ToString(), data.Rows[i]["value"].ToString());
                    
                    
                }
            }

            return param;
        }


        public static string setFormatInterfaceData(int fromatid, DataTable data, string fromatCode, string valuename)
        {

            string param = string.Empty;
            data.DefaultView.Sort = "sort ASC";
            data = data.DefaultView.ToTable();
            data.Columns.Remove("infoid");
            data.Columns.Remove("sort");


            DataRow[] items = data.Select("isRequest=1");

            if (items.Length == 0)
            {
                if (fromatid == 2)
                {
                    foreach (DataRow dr in data.Rows)
                        param += "&" + dr["outField"].ToString() + "=" + dr[valuename].ToString();
                    param = param.Substring(1);
                }
                else if (fromatid == 1 || fromatid == 0)
                {
                    if (!string.IsNullOrEmpty(fromatCode))
                    {
                        param = fromatCode;
                        foreach (DataRow dr in data.Rows)
                            param = FindJsonNode(param, dr["outField"].ToString(), dr[valuename].ToString());
                    }
                    else
                    {
                        foreach (DataRow dr in data.Rows)
                            param += ",\"" + dr["outField"].ToString() + "\":\"" + dr[valuename].ToString() + "\"";

                        param = "{" + param.Substring(1) + "}";
                    }
                }
            }
            else
            {
                if (fromatid == 2)
                {
                    foreach (DataRow dr in items)
                        param += "&" + dr["outField"].ToString() + "=" + dr[valuename].ToString();
                    param = param.Substring(1);
                }
                else if (fromatid == 1 || fromatid == 0)
                {
                    if (!string.IsNullOrEmpty(fromatCode))
                    {
                        param = fromatCode;
                        foreach (DataRow dr in items)
                            param = FindJsonNode(param, dr["outField"].ToString(), dr[valuename].ToString());
                    }
                    else
                    {
                        foreach (DataRow dr in items)
                            param += ",\"" + dr["outField"].ToString() + "\":\"" + dr[valuename].ToString() + "\"";

                        param = "{" + param.Substring(1) + "}";
                    }
                }
            }

            return param;
        }




        /// <summary>
        /// 设置对外输出接口格式
        /// </summary>
        /// <param name="fromatid">格式id</param>
        /// <param name="data">数据</param>
        /// <param name="fromatCode">格式编码</param>
        /// <returns></returns>
        public static string setFormatUpInterfaceData(int fromatid, DataTable data, string fromatCode)
        {
            string param = string.Empty;
            data.DefaultView.Sort = "sort ASC";
            data = data.DefaultView.ToTable();
            data.Columns.Remove("infoid");
            data.Columns.Remove("sort");
            if (fromatid == 2)
            {
                foreach (DataRow dr in data.Rows)
                    param += "&" + dr["outField"].ToString() + "=" + dr["valuedata"].ToString();
                param = param.Substring(1);
            }
            else if (fromatid == 1 || fromatid == 0)
            {
                /*JObject jsonObj = JObject.Parse(fromatCode);
                IEnumerable<JProperty> properties = jsonObj.Properties();
                foreach (JProperty item in properties)
                {
                     string names = item.Name.ToString();
                     DataRow[] result = data.Select("outField='" + names + "'");
                     if(result.Length>0)
                         item.Value = result[0]["value"].ToString();
                }
                param = fromatCode;*/
                for (int i = 0; i < data.Rows.Count; i++)
                {

                    param = FindJsonNodex(param, data.Rows[i]["outField"].ToString(), data.Rows[i]["valuedata"].ToString());


                }
            }

            return param;
        }


        /// <summary>
        /// 设置对外输出上行接口格式
        /// </summary>
        /// <param name="fromatid">格式id</param>
        /// <param name="data">数据</param>
        /// <param name="fromatCode">格式编码</param>
        /// <returns></returns>
        public static string setFormatUpInterfaceDatax(int fromatid, DataTable data, string fromatCode)
        {
            string param = string.Empty;
            data.DefaultView.Sort = "sort ASC";
            data = data.DefaultView.ToTable();
            data.Columns.Remove("infoid");
            data.Columns.Remove("sort");


            DataRow[] items = data.Select("isRequest=1");

            if (items.Length == 0)
            {
                if (fromatid == 2)
                {
                    foreach (DataRow dr in data.Rows)
                        param += "&" + dr["outField"].ToString() + "=" + dr["valuedata"].ToString();
                    param = param.Substring(1);
                }
                else if (fromatid == 1 || fromatid == 0)
                {
                    if (!string.IsNullOrEmpty(fromatCode))
                    {
                        param = fromatCode;
                        foreach (DataRow dr in data.Rows)
                            param = FindJsonNode(param, dr["outField"].ToString(), dr["valuedata"].ToString());
                    }
                    else
                    {
                        foreach (DataRow dr in data.Rows)
                            param += ",\"" + dr["outField"].ToString() + "\":\"" + dr["valuedata"].ToString() + "\"";

                        param = "{" + param.Substring(1) + "}";
                    }
                }
            }
            else
            {
                if (fromatid == 2)
                {
                    foreach (DataRow dr in items)
                        param += "&" + dr["outField"].ToString() + "=" + dr["valuedata"].ToString();
                    param = param.Substring(1);
                }
                else if (fromatid == 1 || fromatid == 0)
                {
                    if (!string.IsNullOrEmpty(fromatCode))
                    {
                        param = fromatCode;
                        foreach (DataRow dr in items)
                            param = FindJsonNode(param, dr["outField"].ToString(), dr["valuedata"].ToString());
                    }
                    else
                    {
                        foreach (DataRow dr in items)
                            param += ",\"" + dr["outField"].ToString() + "\":\"" + dr["valuedata"].ToString()+"\"";

                        param = "{" + param.Substring(1) + "}";
                    }
                }
            }

            return param;
        }



       

        /// <summary>
        /// 设置返回格式内容
        /// </summary>
        /// <param name="fromatid">格式id</param>
        /// <param name="data">数据</param>
        /// <param name="fromatCode">格式编码</param>
        /// <returns></returns>
        public static string setFormatResultData(int fromatid,  DataTable data, string fromatCode)
        {
            string param =null;

            try
            {
               
                if (fromatid == 2)
                {
                    foreach (DataRow dr in data.Rows)
                        param += "&" + dr["outField"].ToString() + "=" + dr["value"].ToString();
                    param = param.Substring(1);
                }
                else if (fromatid == 1 || fromatid == 0)
                {
                    JObject jsonObj = JObject.Parse(fromatCode);
                    IEnumerable<JProperty> properties = jsonObj.Properties();
                    foreach (JProperty item in properties)
                    {
                         string value = item.Value.ToString();
                         DataRow[] result = data.Select("fieldname='" + value + "'");
                         if(result.Length>0)
                             item.Value = result[0]["value"].ToString();
                    }
                    param = jsonObj.ToString();
                   
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), "============>setFormatResultDat设置数据异常[" + e.ToString() + "]");
                return null;
            }

            return param;
        }


        public static JObject getSyncData(DataTable dt)
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

       


        public static void updateSyncData(string StreamingNo, string tabname)
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

        public static int AddSyncData(DataTable dt, string gettab, string settab, string id, string packname, string packdata)
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

                    sql = "INSERT INTO " + settab + "(" + packname + field + ") SELECT top 1 '" + packdata + "'" + field + " from " + gettab + " where datatime between dateadd(day,-2,GETDATE()) and dateadd(day,1,GETDATE()) and correlator='" + id + "'";
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

        /// <summary>
        /// 插入同步数据（使用该方法需要在dt中添加infoid字段）
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="settab"></param>
        /// <param name="packname"></param>
        /// <param name="packdata"></param>
        /// <returns></returns>
        public static int AddSyncData(DataTable dt, string settab, string packname, string packdata)
        {
            string field = "";
            string value = "";
            int flag = 0;
            string sql = null;
            try
            {
                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        field += "," + dr["fieldname"].ToString();
                        value +="," + dr["value"].ToString();
                    }
                    sql = "INSERT INTO " + settab + "(" + packname + field + ") value('" + packdata + "'" + value + ")";
                    flag = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                    if (flag == 0)
                        LogHelper.WriteLog(typeof(Service), "AddSyncData(DataTable dt, string settab, string packname, string packdata)============>插入数据失败[" + sql + "]");
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>AddSyncData(DataTable dt, string settab, string packname, string packdata)插入数据异常[" + e.ToString() + "]");
                return flag;
            }
            return flag;
        }
     


        static string getJsonValue(string name, JObject json)
        {
            IEnumerable<JProperty> properties = json.Properties();
            foreach (JProperty item in properties)
            {
                if (name == item.Name.ToLower())
                    return item.Value.ToString();

            }
            return null;
        }



        public static int AddDataInfo(string tabname, DataTable data)
        {
            int flag = 0;
            string sql = "insert into " + tabname;
            string field = "";
            string value = "";
            try
            {
                foreach (DataRow dr in data.Rows)
                {
                    field += "," + dr["fieldname"].ToString();
                    value += ",'" + dr["value"].ToString() + "'";
                }

                sql += "(" + field.Substring(1) + ") values(" + value.Substring(1) + ")";
                flag = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (flag == 0)
                    LogHelper.WriteLog(typeof(Service), "AddDataInfo============>AddDataInfo插入数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>AddDataInfo获取数据异常[" + e.ToString() + "]");
                return flag;
            }
            return flag;
        }


        /// <summary>
        /// 添加数据
        /// </summary>
        /// <param name="tabname"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public static int AddDataInfox(string tabname, DataTable data)
        {
            int flag = 0;
            string sql = "insert into " + tabname;
            string field = "";
            string value = "";
            try
            {
                /*DataRow[] StreamingNo = data.Select("fieldname='StreamingNo'");
                DataRow[] userOrder = data.Select("fieldname='userOrder'");
                if (userOrder.Length > 0)
                {
                    if (userOrder[0]["value"].ToString() == "")
                        userOrder[0]["value"] = StreamingNo[0]["value"].ToString();
                }
                else
                {
                    field = "userOrder";
                    value = StreamingNo[0]["value"].ToString();
                }*/

                foreach (DataRow dr in data.Rows)
                {
                    field += "," + dr["fieldname"].ToString();
                    value += ",'" + dr["value"].ToString() + "'";
                }

                sql += "(" + field.Substring(1) + ") values(" + value.Substring(1) + ");SELECT SCOPE_IDENTITY()";
                flag = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql),0);
                if (flag == 0)
                    LogHelper.WriteLog(typeof(Service), sql + "============>AddDataInfox插入数据失败");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>AddDataInfox获取数据异常[" + e.ToString() + "]");
                return flag;
            }
            return flag;
        }


        public static DataTable getParsedParam(string configid)
        {

            string sql = null;
            DataTable dt = new DataTable();
            try
            {
                sql = "select * from public_parsed where configid=" + configid;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getParsedParam获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
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



        public static string VerifyParam(string configid, Hashtable param)
        {
            string info = null;
            string sql = null;
            try
            {
                sql = "select * from public_params where configid=" + configid;
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                foreach (DataRow dr in dt.Rows)
                {
                    if (null == param[dr["paramname"].ToString()])
                        return "缺少"+dr["paramname"].ToString()+"参数";
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>VerifyParam获取数据异常[" + e.ToString() + "]");
                return "syserr";
            }
            return info;

        }

        public static DataTable getCompanyID(string key)
        {
            DataTable dt = new DataTable();

            string sql = "select top 1 * from public_ukey where ukey='" + key + "'";

            try
            {
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getCompanyID获取数据异常[" + e.ToString() + "]");
                return dt;
            }

            return dt;
        }


        public static string getActionInfo(DataTable data)
        {
            string sql = null;
            string info = null;
            try
            {
                                             
                DataRow[] row = data.Select("actionitem=1");//查询
                string item = string.Empty;
                if (row.Length > 0) 
                {
                    foreach (DataRow dr in row)
                        item += "and a." + dr["fieldname"].ToString().Replace("fee","price") + "='" + dr["value"].ToString() + "'";

                    sql = "select top 1 a.*,b.address as senderAddress from action as a left join product as b on b.infoid=a.productid where " + item.Substring(4);

                    DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                    if (dt.Rows.Count > 0)
                        info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                } 
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(DataTable data)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static string getActionInfo(int companyid, int fee, int conduitid)
        {
            string sql = null;
            string info = null;
            try
            {
                sql = "select top 1 a.productid,a.ordered as message,b.address as senderAddress,a.buyid,a.infoid as actionid,REPLACE(REPLACE(REPLACE(REPLACE(convert(varchar(50), sysdatetime()),'-',''),':',''),' ',''),'.','') as transactionId"
                    + " from action as a left join product as b on b.infoid=a.productid"
                    + " where companyid=" + companyid + " and price=" + fee + " and a.conduitid=" + conduitid + " and syncstart=1";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(int companyid, int fee, int conduitid)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static string getActionInfo(int companyid, int conduitid, string product)
        {
            string sql = null;
            string info = null;
            try
            {
                sql = "select top 1 a.*,b.address from action as a left join product as b on b.infoid=a.productid"
                    + " where companyid=" + companyid + " and productCode='" + product + "' and a.conduitid=" + conduitid + " and syncstart=1";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(int companyid, int conduitid, string product)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static string getActionInfo(int companyid, int conduitid, int configid, int fee)
        {
            string sql = null;
            string info = null;
            try
            {
                sql = "select top 1 a.productid,a.ordered as message,b.address as senderAddress,a.buyid,a.infoid as actionid,REPLACE(REPLACE(REPLACE(REPLACE(convert(varchar(50), sysdatetime()),'-',''),':',''),' ',''),'.','') as transactionId"
                    + " from action as a left join product as b on b.infoid=a.productid"
                    + " where companyid=" + companyid + " and price=" + fee + " and a.conduitid=" + conduitid + " and a.configid=" + configid + " and  syncstart=1";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(int companyid, int fee, int conduitid)获取数据异常[" + e.ToString() + "]");
                return info;
            }

            return info;
        }


        public static string getActionInfo(int companyid, int conduitid)
        {
            string info = null;
            string sql = "SELECT top 1 a.* FROM action as a where a.conduitid=" + conduitid + " and  a.companyid =" + companyid;
            try
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                else
                    LogHelper.WriteLog(typeof(Service), "============>getActionInfo(int companyid, int conduitid)获取数据失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(int companyid, int conduitid)获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }

        public static string getActionInfo(string code, int conduitid)
        {
            string info = null;
            //string sql = "SELECT top 1 a.*,b.address FROM action as a left join product as b on a.productID=b.infoid where a.conduitid=" + conduitid + " and  a.ordered =(SELECT max(ordered) FROM action where ordered=left('" + code + "',LEN(ordered))) or a.unsubscribe=(SELECT max(unsubscribe) FROM action where unsubscribe=left('"+code+"',LEN(unsubscribe)))";
            string sql="SELECT top 1 a.*,b.address FROM action as a left join product as b on a.productID=b.infoid where a.conduitid=" + conduitid + " and (a.ordered =(SELECT max(ordered) FROM action where ordered=left('"+code+"',LEN(ordered))) or unsubscribe='"+code+"')";
            
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


        public static string getActionMessage(string mobile, int conduitID, int compnayID)
        {
            string info = null;
            string sql = "SELECT top 1 message FROM public_notify_2017 where datatime>dateadd(MONTH,-6,GETDATE()) and mobile='" + mobile + "'  and conduitID=" + conduitID + " and companyID=" + compnayID + " and optype=0";
            try
            {
                object message = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (null != message)
                    info = message.ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(string message)获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }



        public static string getActionMessage(string mobile, int conduitID)
        {
            string info = null;
            string sql = "SELECT top 1 message FROM public_notify_2017 where mobile='" + mobile + "'  and conduitID=" + conduitID + " and optype=0";
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (null!= old)
                    info = old.ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(string message)获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }



        public static string getActionCompany(string mobile, int conduitID)
        {
            string info = null;
            string sql = "SELECT top 1 message,companyid FROM public_notify_2017 where mobile='" + mobile + "'  and conduitID=" + conduitID + " and optype=0";
            try
            {
                DataTable message = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (message.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(message, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getActionInfo(string message)获取数据异常[" + e.ToString() + "]");
            }

            return info;
        }



       


        public static string getTransactionId()
        {
            string info = null;
            string sql = "select REPLACE(REPLACE(REPLACE(REPLACE(convert(varchar(50), sysdatetime()),'-',''),':',''),' ',''),'.','') as transactionId";

            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = old.ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getTransactionId获取数据异常[" + e.ToString() + "]");
                return info;
            }
            return info;
        }


        public static string getJsonData(JObject get, JObject set)
        {
            try
            {
                IEnumerable<JProperty> properties = set.Properties();
                foreach (JProperty item in properties)
                {
                    if (null != get[item.Name])
                        set[item.Name] = get[item.Name].ToString();
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service),
                    "\r\n================================================================="
                    + "\r\n getJsonData获取数据异常[" + e.ToString() + "]"
                    + "\r\n get[" + get + "]"
                    + "\r\n set[" + set + "]"
                    + "\r\n================================================================");
                return null;
            }

            return set.ToString();
        }




        public static int setDeductPoint(int conduitID, int companyID, int point, string message, string buyid)
        {
            int point1 = getPoint(conduitID, companyID, message, buyid);
            int point2 = getAllPoint(conduitID, companyID, message, buyid);
            if (point1 > 0)
            {
                double data = Math.Round((double)(point2 - point1) / point2, 3);
                int i = (int)(data * 100);
                if (i < point)
                    return 1;
            }
            return 0;
        }


        /// <summary>
        /// 获取用户定购金额
        /// </summary>
        /// <param name="message">分配的指令</param>
        /// <returns></returns>
        static int getAllPoint(int conduitID, int companyID, string message, string buyid)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from public_notify_2017 WITH(NOLOCK) where message like '" + message + "%' and conduitID=" + conduitID + " and companyid=" + companyID + " and buyid=" + buyid + " and optype=0 and resultCode=0 and datatime between " + ts.GetToday();
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>2017getAllPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        static int getPoint(int conduitID, int companyID, string message, string buyid)
        {
            int info = 0;
            Time ts = new Time();
            string sql = "select count(infoid) as point from public_sync_2017 where message like '" + message + "%' and conduitID=" + conduitID + " and companyid=" + companyID + " and  buyid=" + buyid + " and optype=0 and resultCode=0 and datatime between " + ts.GetToday();
                      try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getPoint获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        /// <summary>
        /// 用户订单过滤
        /// </summary>
        /// <param name="userOrder"></param>
        /// <returns></returns>
        public static int getOrderInfo(string userOrder)
        {
            int info = 0;

            string sql = "select top 1 infoid as id from public_notify_2017 where datatime>dateadd(day,-30,GETDATE()) and userorder='" + userOrder + "'";
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getOrderInfo获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }

        /// <summary>
        /// 用户订单过滤
        /// </summary>
        /// <param name="data"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static int getOrderInfo(DataTable data, string table)
        {
             int info=0;
             DataRow[] row = data.Select("filter=1");//查询
             if(row.Length>0)
             {
                 for (int i = 0; i < row.Length; i++)
                 {
                     string sql = "select count(infoid) as id from " + table + " where " + row[i]["fieldname"].ToString() + "='" + row[i]["value"].ToString() + "' and conduitID=" + row[i]["conduitID"].ToString();
                     try
                     {
                         object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                         if (DBNull.Value != old)
                         {
                             info = Convert.ToInt32(old);
                             break;
                         }
                     }
                     catch (Exception e)
                     {
                         LogHelper.WriteLog(typeof(Service), sql + "============>getOrderInfo(DataTable data, string table)获取数据异常[" + e.ToString() + "]");
                         return 0;
                     }

                 }
             }
             return info;
        }

        /// <summary>
        /// 用户订单过滤
        /// </summary>
        /// <param name="userOrder"></param>
        /// <param name="conduitID"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static int getOrderInfo(string userOrder, int conduitID, string table)
        {
            int info = 0;

            string sql = "select top 1 infoid as id from " + table + " where datatime>dateadd(day,-30,GETDATE()) and userorder='" + userOrder + "' and conduitID=" + conduitID;
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getOrderInfo获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }



        public static int getOrderInfo(string userOrder, int conduitID, string table, string field)
        {
            int info = 0;

            string sql = string.Format("select top 1 infoid as id from {0} where datatime>dateadd(day,-30,GETDATE()) and {1}='{2}' and conduitID={3}", table, field, userOrder, conduitID);
            try
            {
                object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (DBNull.Value != old)
                    info = Convert.ToInt32(old);
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>getOrderInfo获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }

        /// <summary>
        /// 查询订单号
        /// </summary>
        /// <param name="userOrder"></param>
        /// <param name="conduitID"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static DataTable getOrderInfox(string userOrder, int conduitID, string table)
        {
            DataTable info = new DataTable();

            string sql = string.Format("select top 1 a.infoid as orderid,(select top 1 infoid from {0} where userOrder=a.userOrder and datatime>dateadd(day,-30,GETDATE()) and OPType=1) as cancelid from {1} as a where a.userOrder='{2}' and a.datatime>dateadd(day,-30,GETDATE()) and a.conduitID={3} and a.OPType=0", table, table, userOrder, conduitID);
            try
            {
                 info = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>DataTable getOrderInfo(string userOrder, int conduitID, string table)获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }


        public static DataTable getOrderInfox(string userOrder, int conduitID, string table, string field)
        {
            DataTable info = new DataTable();

            string sql = "select top 1 a.infoid as orderid,(select top 1 infoid from " + table + " where " + field + "=a." + field + " and datatime>dateadd(day,-30,GETDATE()) and OPType=1) as cancelid from " + table + " as a where a." + field + "='" + userOrder + "' and a.datatime>dateadd(day,-30,GETDATE()) and a.conduitID=" + conduitID + " and a.OPType=0";
            try
            {
                info = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(Service), sql + "============>DataTable getOrderInfo(string userOrder, int conduitID, string table)获取数据异常[" + e.ToString() + "]");
            }
            return info;
        }

        /// <summary>
        /// 数据包内容替换
        /// </summary>
        /// <param name="pack"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static string setPackReplace(packInfo pack, DataTable dt)
        {
            string info = null;
            DataRow[] dr = dt.Select("packReplace<>''");
            if (dr.Length > 0)
            {
                if (dt.Rows[0]["format"].ToString() == "0")
                {
                    JObject jsonObj = JObject.Parse(dr[0]["packReplace"].ToString());
                    IEnumerable<JProperty> properties = jsonObj.Properties();
                    foreach (JProperty item in properties)
                    {
                        string key = item.Name.ToString();
                        if (key == "param")
                        {

                        }
                    }
                }
            }

            return info;
        }


       /// <summary>
       /// 量满通知
       /// </summary>
       /// <param name="name"></param>
       /// <param name="notify"></param>
       /// <param name="smtpinfo"></param>
       /// <param name="over"></param>
       public static void overNotify(string name,string notify,string smtpinfo, string over)
       {
            bool flag=false;
           
            JObject smtp=JObject.Parse(smtpinfo);
            
            string[] record =notify.Split(',') ;

            for(int i=0;i<record.Length;i++)
                flag = Utils.SendSMTPEMail(smtp["smtp"].ToString(), smtp["from"].ToString(), smtp["password"].ToString(), record[i], name, name+over);
               

       }


        /// <summary>
        /// 获取字典表对应数据
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
       public static string getDictInfo(string sql)
       {
           string info = string.Empty;
          
           try
           {
               DataTable tb = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
               if (tb.Rows.Count > 0)
               {
                   foreach (DataRow dr in tb.Rows)
                       info+=",\""+dr["name"].ToString()+"\":"+dr["value"].ToString()+"\"";
               }
           }
           catch (Exception e)
           {
               LogHelper.WriteLog(typeof(Service), sql + "============>getDictInfo获取数据异常[" + e.ToString() + "]");
               return null;
           }
           return "{"+info.Substring(1)+"}";
       }

     public static string conduitName(int conduitID)
     {
         
         string info = null;
         string sql = "select names from conduit where infoid=" + conduitID;

         try
         {
             object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
             if (DBNull.Value != old)
                 info = old.ToString();
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>conduitName获取数据异常[" + e.ToString() + "]");
             return info;
         }
         return info;


     }

     /// <summary>
     /// 按省份扣留
     /// </summary>
     /// <param name="area"></param>
     /// <param name="setpoint"></param>
     /// <returns></returns>
     public static bool AreaPoint(string area, string setpoint)
     {
         bool state =true;
         string data = "[" + setpoint + "]";
         JArray point = JArray.Parse(data);
         foreach (JObject items in point)
         {
             string value =items["value"].ToString();
             if (area.IndexOf(value) > -1)
             {
                 state = Utils.RandNumber(Convert.ToInt32(items["point"].ToString()));
                 break;
             }
         }
         return state;
     }

     public static bool AreaPointx(string area, string setpoint)
     {
         bool state = true;
         
         JArray point = JArray.Parse(setpoint);
         foreach (JObject items in point)
         {
             string value = items["value"].ToString();
             if (area.IndexOf(value) > -1)
             {
                 state = Utils.RandNumber(Convert.ToInt32(items["point"].ToString()));
                 break;
             }
         }
         return state;
     }


        /// <summary>
        /// 获取资费
        /// </summary>
        /// <param name="productCode"></param>
        /// <returns></returns>
     public static int getActionFee(string productCode)
     {
         int fee = 0;
         string sql = "select price as fee from action where productCode='"+ productCode+"'";

         try
         {
             object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
             if (DBNull.Value != old)
                 fee = Convert.ToInt32(old);
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>conduitName获取数据异常[" + e.ToString() + "]");
             return 0;
         }
         return fee;
     }


     public static int getActionFee(string productCode,int companyid, int conduitid)
     {
         int fee = 0;
         string sql = "select price as fee from action where productCode='" + productCode + "' and companyid=" + companyid + " and conduitid=" + conduitid;

         try
         {
             object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
             if (DBNull.Value != old)
                 fee = Convert.ToInt32(old);
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>conduitName获取数据异常[" + e.ToString() + "]");
             return 0;
         }
         return fee;
     }
    

     /// <summary>
     /// 同步验证
     /// </summary>
     /// <param name="action"></param>
     /// <param name="data"></param>
     /// <returns></returns>
     public static bool syncVerify(string action, DataTable data, DataTable dt)
     {
         bool flag = false;

         DataRow[] OPType = data.Select("fieldname='OPType'");
         DataRow[] buyID = data.Select("fieldname='buyID'");
         DataRow[] mobile = data.Select("fieldname='mobile'");
         DataRow[] imsi = data.Select("fieldname='imsi'");

         JObject ainfo = JObject.Parse(action);

         if (ainfo["syncStart"].ToString() == "0")
             return flag;
         
         if (ainfo["syncFlag"].ToString() == "0")
             return flag;

         if (string.IsNullOrEmpty(ainfo["syncUrl"].ToString()))
             return flag;

         if (buyID.Length == 0)
             return flag;

         if (buyID[0]["value"].ToString() == "1")
             return true;

          //查询是否有同步数据的配置
          DataTable next = Service.getConfigInfo(Convert.ToInt32(dt.Rows[0]["infoid"]), Convert.ToInt32(dt.Rows[0]["step"])+1);
          if (next.Rows.Count == 0)
             return flag;


          int sync = searchSyncData(data.Rows[0]["mobile"].ToString(), Convert.ToInt32(ainfo["conduitID"].ToString()), Convert.ToInt32(ainfo["companyID"].ToString()), next.Rows[0]["tablename"].ToString());
          if (sync == 2)//订购和退订都已同步
              return flag;
          else if (sync == 1)
          {
              if (OPType[0]["value"].ToString() == "1")
              {
                  if (ainfo["optypeid"].ToString() == "0")//同步退订是否打开
                      return flag;

                  int pid = search72hours(mobile[0]["value"].ToString(), imsi[0]["value"].ToString(),dt.Rows[0]["tablename"].ToString());//查询订购时间是否在72小时内
                  if (pid == 0)
                      return true;
              }
          }

          return flag;
         
     }

     /// <summary>
     /// 获取通道id
     /// </summary>
     /// <param name="productcode"></param>
     /// <param name="companyid"></param>
     /// <returns></returns>
     public static int getCondutIDInfo(string productcode, int companyid)
     {
         int info = 0;

         string sql = "SELECT top 1 conduitid FROM action where productcode='" + productcode + "' and  companyid=" + companyid;
         try
         {
             object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
             if (DBNull.Value != old)
                 info = Convert.ToInt32(old);
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>getCondutIDInfo获取数据异常[" + e.ToString() + "]");
             return 0;
         }
         return info;
     }


     public static int getCondutIDInfo(string productcode)
     {
         int info = 0;

         string sql = "SELECT top 1 conduitid FROM action where productcode='" + productcode + "'";
         try
         {
             object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
             if (DBNull.Value != old)
                 info = Convert.ToInt32(old);
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>getCondutIDInfo获取数据异常[" + e.ToString() + "]");
             return 0;
         }
         return info;
     }


    /// <summary>
    /// 获取支付key
    /// </summary>
    /// <param name="companyid"></param>
    /// <param name="conduitid"></param>
    /// <param name="productcode"></param>
    /// <returns></returns>
    public static string getPayUKEY(int companyid, int conduitid, string productcode)
    {
         string ukey=string.Empty;

         string sql = "SELECT top 1 code FROM action where companyid=" + companyid + " and conduitid=" + conduitid + " and productcode='" + productcode + "'";
         try
         {
             DataTable tab = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
             if (tab.Rows.Count>0)
                 ukey = tab.Rows[0][0].ToString();
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>payUKEY获取数据异常[" + e.ToString() + "]");
             return "err";
         }
         return ukey;
    }



    public static string getPayUKEY(int conduitid, string productcode)
    {
        string ukey = string.Empty;

        string sql = "SELECT top 1 code FROM action where conduitid=" + conduitid + " and productcode='" + productcode + "'";
        try
        {
            DataTable tab = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (tab.Rows.Count > 0)
                ukey = tab.Rows[0][0].ToString();
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>payUKEY获取数据异常[" + e.ToString() + "]");
            return "err";
        }
        return ukey;
    }



   
    /// <summary>
    /// 获取支付加密字段
    /// </summary>
    /// <param name="conduitid"></param>
    /// <param name="configid"></param>
    /// <returns></returns>

    public static DataTable getPayVerifiField(int conduitid, int configid)
    {
         DataTable tab=new DataTable();

         string sql = "SELECT * FROM public_verification where conduitid="+conduitid+" and configid="+configid+" order by sort";
         try
         {
             tab = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
           
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(Service), sql + "============>getPayVerifiField获取数据异常[" + e.ToString() + "]");
           
         }
         return tab;
    }

    public static int payVerification(int conduitid, Hashtable data, DataTable md5field)
    {
        int flag = 0;
        string value = "";
        try
        {
            string ukey = getPayUKEY(Convert.ToInt32(data["companyID"]), conduitid, data["productCode"].ToString());
            
            //DataTable tab = getPayVerifiField(Convert.ToInt32(data["companyID"]), conduitid, configid);

            if (string.IsNullOrEmpty(ukey))
                return flag;
            if (ukey == "err")
                return -1;
            //if (tab.Rows.Count == 0)
                //return flag;
            if (null == data["sign"])
                return flag;
            foreach (DataRow dr in md5field.Rows)
            {
                if (null != data[dr["field"].ToString()])
                    value += "&" + dr["field"].ToString() + "=" + data[dr["field"].ToString()].ToString();

            }
            value = Utils.MD5(value.Substring(1) + "&key="+ukey, System.Text.Encoding.UTF8);

            if (value.ToUpper() != data["sign"].ToString())
                return flag;
            flag=1;
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>payVerification获取数据异常[" + e.ToString() + "]");
            return -1;
        }
        return flag;
    }



    public static int wechatVerification(int conduitid, Hashtable data, DataTable md5field)
    {
        int flag = 0;
        string value = "";
        try
        {
            string ukey = getPayUKEY(Convert.ToInt32(data["companyID"]), conduitid, data["productCode"].ToString());

            //DataTable tab = getPayVerifiField(Convert.ToInt32(data["companyID"]), conduitid, configid);

            if (string.IsNullOrEmpty(ukey))
                return flag;
            if (ukey == "err")
                return -1;
            //if (tab.Rows.Count == 0)
            //return flag;
            if (null == data["sign"])
                return flag;
            foreach (DataRow dr in md5field.Rows)
            {
                if (null != data[dr["field"].ToString()])
                    value +=data[dr["field"].ToString()].ToString();

            }
            value = Utils.MD5(value+ukey, System.Text.Encoding.UTF8);

            if (value.ToUpper() != data["sign"].ToString())
                return flag;
            flag = 1;
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>payVerification获取数据异常[" + e.ToString() + "]");
            return -1;
        }
        return flag;
    }
   
    /// <summary>
    /// 设置支付加密字段值
    /// </summary>
    /// <param name="sync"></param>
    /// <param name="data"></param>
    /// <param name="configid"></param>
    /// <returns></returns>
    public static bool setPayVerifiValue(DataTable sync, string code)
    {
        bool flag = false;
        string value = "";
        try
        {
            string ukey = string.Empty;

            if (string.IsNullOrEmpty(ukey))
                return flag;
            else
                ukey = "&key=" + code;
            
            DataRow[] sing = sync.Select("setmd5=1");
            if (sing.Length == 0)
                return flag;

            foreach (DataRow dr in sync.Rows)
                value += "&" + dr["outField"].ToString() + "=" + dr["value"].ToString();

            value = Utils.MD5(value.Substring(1) + ukey, System.Text.Encoding.UTF8);

            sing[0]["value"] = value.ToUpper();
           
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>setPayVerifiValue获取数据异常[" + e.ToString() + "]");
            return flag;
        }
        return true;
    }


    
    /*public static Hashtable parsingReqeustParams(Hashtable ht, DataTable data, string datapack, int formatid)
    {
        
        foreach (DataRow dr in data.Rows)
        {
            
        }
    }*/



    public static int LimitInfo(int conduitid,int productid,string area)
    {
        int info = 0;

        string sql = "SELECT count(DISTINCT(a.mobile)) FROM public_notify_2017 WITH(NOLOCK) where area like '" + area + "%' and datatime between CONVERT(varchar(10),GETUTCDATE(),120) and CONVERT(varchar(10),DATEADD(dd,+1,GETUTCDATE()),120) and resultCode=0 and conduitid=" + conduitid + " and b.productid=" + productid;
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>LimitInfo获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }

    /// <summary>
    /// 获取限制省份成功数据
    /// </summary>
    /// <param name="conduitid"></param>
    /// <param name="productcode"></param>
    /// <param name="area"></param>
    /// <param name="stime"></param>
    /// <param name="etime"></param>
    /// <returns></returns>
    public static int getLimitAreaTotal(int conduitid, string productcode, string area, string stime, string etime)
    {
        int info = 0;

        string sql = "SELECT count(DISTINCT(a.mobile)) FROM public_notify_2017 as a WITH(NOLOCK) left join action as b on b.productid=a.productid where a.area like '" + area + "%' and a.datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and a.resultCode=0 and a.conduitid=" + conduitid + " and b.productcode='"+ productcode+"'";
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>LimitInfo获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }




    public static int getLimitAreaTotal(int conduitid, string productcode, string area, string stime, string etime, bool distinct, string countfield)
    {
        int info = 0;

        string paramstr="";
        string countitem="count(a.mobile)";

        if (!string.IsNullOrEmpty(countfield))
            countitem = "count(a." + countfield + ")";

        if (!string.IsNullOrEmpty(productcode))
            paramstr = " and a.productcode='" + productcode+"'";

        if (distinct)
            countitem =countitem.Replace("count(","count(DISTINCT(").Replace(")","))");

        string sql = "SELECT " + countitem + " FROM public_notify_2017 as a WITH(NOLOCK) where a.area like '" + area + "%' and a.datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and a.resultCode=0 and a.conduitid=" + conduitid + paramstr;
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitAreaTotal获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }


    public static int getLimitAreaTotal(int conduitid, int companyid, string productcode, string area, string stime, string etime, bool distinct, string countfield, string tablename)
    {
        int info = 0;

        string paramstr = string.Empty;
        string countitem = string.Empty;


        if (!string.IsNullOrEmpty(countfield))
            countitem = "count(" + countfield + ")";
        else
            countitem = "count(userOrder)";
        if (!string.IsNullOrEmpty(productcode))
            paramstr += " and productcode='" + productcode + "'";

        if(!string.IsNullOrEmpty(area))
            paramstr +=" and area like '" + area + "%'";

        if (companyid>0)
            paramstr += " and companyid='" + companyid + "'";
        if (distinct)
            countitem = countitem.Replace("count(", "count(DISTINCT(").Replace(")", "))");

        string sql = "SELECT " + countitem + " FROM " + tablename + " as a WITH(NOLOCK) where datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and resultCode=0 and OPType=0 and conduitid=" + conduitid + paramstr;
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitAreaTotal获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }



    public static int getLimitAreaTotal(int conduitid, int companyid, int productid, string area, string stime, string etime, bool distinct, string countfield, string tablename)
    {
        int info = 0;

        string paramstr = string.Empty;
        string countitem = string.Empty;

        if (!string.IsNullOrEmpty(countfield))
            countitem = "count(" + countfield + ")";
        else
            countitem = "count(IMSI)";

        if (productid>0)
            paramstr += " and productid=" + productid;

        if (!string.IsNullOrEmpty(area))
            paramstr += " and area like '" + area + "%'";

        if (companyid > 0)
            paramstr += " and companyid='" + companyid + "'";
        if (distinct)
            countitem = countitem.Replace("count(", "count(DISTINCT(").Replace(")", "))");

        string sql = "SELECT " + countitem + " FROM " + tablename + " as a WITH(NOLOCK) where datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and resultCode=0 and OPType=0 and conduitid=" + conduitid + paramstr;
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitAreaTotal获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }
    
    /// <summary>
    /// 获取日月限制金额成功数据
    /// </summary>
    /// <param name="conduitid"></param>
    /// <param name="companyid"></param>
    /// <param name="productcode"></param>
    /// <param name="area"></param>
    /// <param name="stime"></param>
    /// <param name="etime"></param>
    /// <param name="groupfield"></param>
    /// <param name="tablename"></param>
    /// <returns></returns>
    public static int getLimitFeeTotal(int conduitid, int companyid, string productcode, string area, string stime, string etime, string groupfield, string tablename)
    {
        int info = 0;

        string paramstr = string.Empty;
        string countitem = string.Empty;

        if (!string.IsNullOrEmpty(groupfield))
            countitem = "group by " + groupfield;

        if (!string.IsNullOrEmpty(area))
            paramstr += " and area like '" + area + "%'";

        if (!string.IsNullOrEmpty(productcode))
            paramstr += " and productcode='" + productcode + "'";

        if (companyid > 0)
            paramstr += " and companyid='" + companyid + "'";

        string sql = "SELECT sum(tab.fee) FROM(SELECT fee FROM " + tablename + " WITH(NOLOCK) where datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and resultCode=0 and OPType=0 and conduitid=" + conduitid + paramstr + ") AS tab";
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitAreaTotal获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }


    public static int getLimitFeeTotal(int conduitid, int companyid, int productid, string area, string stime, string etime, string groupfield, string tablename)
    {
        int info = 0;

        string paramstr = string.Empty;
        string countitem = string.Empty;

        if (!string.IsNullOrEmpty(groupfield))
            countitem = "group by " + groupfield;

        if (!string.IsNullOrEmpty(area))
            paramstr += " and area like '" + area + "%'";

        if (productid>0)
            paramstr += " and productid=" + productid;

        if (companyid > 0)
            paramstr += " and companyid=" + companyid;

        string sql = "SELECT sum(tab.fee) FROM(SELECT fee FROM " + tablename + " WITH(NOLOCK) where datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and resultCode=0 and OPType=0 and conduitid=" + conduitid + paramstr + ") AS tab";
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                info = Convert.ToInt32(old);
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitAreaTotal获取数据异常[" + e.ToString() + "]");
            return 0;
        }
        return info;
    }


    /// <summary>
    /// 获取量满公司名称(该方法零时使用 后期改为 action 和 company 的联合查询)
    /// </summary>
    /// <param name="conduitid"></param>
    /// <param name="productid"></param>
    /// <param name="stime"></param>
    /// <param name="etime"></param>
    /// <param name="datatable"></param>
    /// <returns></returns>
    public static string getLimitCompany(int conduitid, int productid,string stime,string etime,string datatable)
    {
        string info = null;
        string sql = "SELECT b.name FROM " + datatable + " as a WITH(NOLOCK) left join company as b on b.infoid=a.companyID where a.datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and a.conduitid=" + conduitid + " and a.productid=" + productid + " and a.codeflag=0  group by b.name";
        try
        {
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
                info = JsonConvert.SerializeObject(dt, new DataTableConverter());

        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitCompany获取数据异常[" + e.ToString() + "]");
            return info;
        }

        return info;
    }

    public static string getLimitCompany(int conduitid, int productid, string area, string stime, string etime, string datatable)
    {
        string info = null;
        string sql = "SELECT b.name FROM " + datatable + " as a WITH(NOLOCK) left join company as b on b.infoid=a.companyID where a.area like '" + area + "%' and a.datatime between '" + stime + "' and DATEADD(MINUTE,+1,'" + etime + "') and a.conduitid=" + conduitid + " and a.productid=" + productid + " and a.codeflag=0  group by b.name";
        try
        {
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
                info = JsonConvert.SerializeObject(dt, new DataTableConverter());

        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getLimitCompany获取数据异常[" + e.ToString() + "]");
            return info;
        }

        return info;
    }


    /// <summary>
    /// 限量规则
    /// </summary>
    /// <param name="limit">规则内容json字符串</param>
    /// <param name="area">号码地区</param>
    /// <param name="conduitid">通道id</param>
    /// <param name="productcode">产品编码</param>
    /// <param name="stime">业务开始时间</param>
    /// <param name="etime">业务结束时间</param>
    /// <param name="distinct">是否过滤重复</param>
    /// <param name="countfield">统计字段</param>
    /// <returns></returns>
    public static string LimitRules(object limit, string area, int conduitid, int companyid, string productcode, string stime, string etime, bool distinct, string countfield,string tabname)
    {
        string info = null;
        string sday = null;
        string eday = null;
        string shhss = " 00:00";
        string ehhss = " 23:59";
        string smonth = null;
        string emonth = null;
        try
        {
            string[] ls = area.Split(' ');
            JArray rules = JArray.Parse(limit.ToString());

            if (limit.ToString().IndexOf(ls[0]) == -1)
                return info;

            if (string.IsNullOrEmpty(stime) && string.IsNullOrEmpty(etime))
            {
                string date = DateTime.Now.ToString("yyyy-MM-dd");
                sday = date + shhss;
                eday = date + ehhss;

                DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(-1);
                smonth = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01" + shhss;
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }
            else
            {
                sday = stime;
                eday = etime;

                DateTime dtime = Convert.ToDateTime(stime);
                smonth = dtime.Year.ToString() + "-" + dtime.Month.ToString() + "-01" + shhss;
                DateTime monthDays = new DateTime(dtime.Year, (dtime.Month), 1).AddMonths(1).AddDays(-1);
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }

            foreach (JObject items in rules)
            {
                if (items["area"].ToString().IndexOf(ls[0])>-1)
                {

                    if (Convert.ToInt32(items["month"]) > 0)
                    {
                        int month = Service.getLimitAreaTotal(conduitid, companyid, productcode, items["area"].ToString(), smonth, emonth, distinct, countfield, tabname);
                        if (month >= Convert.ToInt32(items["month"]))
                        {
                            items.Remove("day");
                            items.Remove("dayfee");
                            items["stime"] = smonth;
                            items["etime"] = emonth;
                            items.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                            items.Add(new JProperty("key", conduitid + "m"));
                            info = items.ToString();
                            break;
                        }
                    }
                    if (Convert.ToInt32(items["day"]) > 0)
                    {
                        int day = Service.getLimitAreaTotal(conduitid, companyid, productcode, items["area"].ToString(), sday, eday, distinct, countfield, tabname);
                        if (day >= Convert.ToInt32(items["day"]))
                        {
                            items.Remove("month");
                            items.Remove("monthfee");
                            items["stime"] = sday;
                            items["etime"] = eday;
                            items.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                            items.Add(new JProperty("key", conduitid + "d"));
                            info = items.ToString();
                            break;
                        }
                    }

                    break;
                }
            }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service),"============>LimitRules获取数据异常[" + e.ToString() + "]");
            return info;
        }
        return info;
    }



    public static string LimitRules(object limit, string area, int conduitid, int companyid, string productcode, string stime, string etime, string groupfield, string tabname)
    {
        string info = null;
        string sday = null;
        string eday = null;
        string shhss = " 00:00";
        string ehhss = " 23:59";
        string smonth = null;
        string emonth = null;
        string areainfo=null;
        try
        {
            if (!string.IsNullOrEmpty(area))
            {
                string[] ls = area.Split(' ');
                if (string.IsNullOrEmpty(ls[0].Trim()))
                    return info;

              
                if (limit.ToString().IndexOf(areainfo) == -1)
                    return info;

                areainfo = ls[0];
            }

            JObject rules = JObject.Parse(limit.ToString());

           

            if (string.IsNullOrEmpty(stime) && string.IsNullOrEmpty(etime))
            {
                string date = DateTime.Now.ToString("yyyy-MM-dd");
                sday = date + shhss;
                eday = date + ehhss;

                DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(-1);
                smonth = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01" + shhss;
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }
            else
            {
                sday = stime;
                eday = etime;

                DateTime dtime = Convert.ToDateTime(stime);
                smonth = dtime.Year.ToString() + "-" + dtime.Month.ToString() + "-01" + shhss;
                DateTime monthDays = new DateTime(dtime.Year, (dtime.Month), 1).AddMonths(1).AddDays(-1);
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }


                   if (!string.IsNullOrEmpty(rules["monthfee"].ToString()) && Convert.ToInt32(rules["monthfee"]) > 0)
                    {
                        int month = Service.getLimitFeeTotal(conduitid, companyid, productcode, areainfo, smonth, emonth, groupfield, tabname);
                        if (month >= Convert.ToInt32(rules["monthfee"]))
                        {
                            rules.Remove("limit");
                            rules.Remove("dayfee");

                            rules.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                            rules.Add(new JProperty("key", conduitid + "m"));
                            info = rules.ToString();
                           
                        }
                    }
                   else if (!string.IsNullOrEmpty(rules["dayfee"].ToString()) && Convert.ToInt32(rules["dayfee"]) > 0)
                    {
                        int day = Service.getLimitFeeTotal(conduitid, companyid, productcode, areainfo, sday, eday, groupfield, tabname);
                        if (day >= Convert.ToInt32(rules["dayfee"]))
                        {
                            rules.Remove("limit");
                            rules.Remove("monthfee");
                            rules.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                            rules.Add(new JProperty("key", conduitid + "d"));
                            info = rules.ToString();
                           
                        }
                    }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>LimitRules获取数据异常[" + e.ToString() + "]");
            return info;
        }
        return info;
    }


    public static string LimitRules(object limit, string area, int conduitid, int companyid, string productcode, string stime, string etime, string tabname)
    {
        string info = null;
        string sday = null;
        string eday = null;
        string shhss = " 00:00";
        string ehhss = " 23:59";
        string smonth = null;
        string emonth = null;
        string areainfo = null;
        string groupfield = null;
        try
        {
            if (string.IsNullOrEmpty(area))
                return info;

            JObject rules = JObject.Parse(limit.ToString());

            if (null != rules["filter"])
                groupfield = rules["filter"].ToString();

            if (string.IsNullOrEmpty(stime) && string.IsNullOrEmpty(etime))
            {
                string date = DateTime.Now.ToString("yyyy-MM-dd");
                sday = date + shhss;
                eday = date + ehhss;

                DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(-1);
                smonth = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01" + shhss;
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }
            else
            {
                sday = stime;
                eday = etime;

                DateTime dtime = Convert.ToDateTime(stime);
                smonth = dtime.Year.ToString() + "-" + dtime.Month.ToString() + "-01" + shhss;
                DateTime monthDays = new DateTime(dtime.Year, (dtime.Month), 1).AddMonths(1).AddDays(-1);
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }

            if (!string.IsNullOrEmpty(rules["monthfee"].ToString()) && Convert.ToInt32(rules["monthfee"]) > 0)
            {
                int month = Service.getLimitFeeTotal(conduitid, companyid, productcode, areainfo, smonth, emonth, groupfield, tabname);
                if (month >= Convert.ToInt32(rules["monthfee"]))
                {
                    rules.Remove("limit");
                    rules.Remove("dayfee");

                    rules.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                    rules.Add(new JProperty("key", conduitid + "m"));
                    info = rules.ToString();
                }
            }
            else if (!string.IsNullOrEmpty(rules["dayfee"].ToString()) && Convert.ToInt32(rules["dayfee"]) > 0)
            {
                int day = Service.getLimitFeeTotal(conduitid, companyid, productcode, areainfo, sday, eday, groupfield, tabname);
                if (day >= Convert.ToInt32(rules["dayfee"]))
                {
                    rules.Remove("limit");
                    rules.Remove("monthfee");
                    rules.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                    rules.Add(new JProperty("key", conduitid + "d"));
                    info = rules.ToString();
                }
            }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>LimitRules获取数据异常[" + e.ToString() + "]");
            return info;
        }
        return info;
    }



    /// <summary>
    /// 总量限制
    /// </summary>
    /// <param name="limit"></param>
    /// <param name="conduitid"></param>
    /// <param name="companyid"></param>
    /// <param name="productid"></param>
    /// <param name="stime"></param>
    /// <param name="etime"></param>
    /// <param name="tabname"></param>
    /// <returns></returns>
    public static string LimitRules(object limit, int conduitid, int companyid, int productid, string stime, string etime, string tabname)
    {
        string info = null;
        string sday = null;
        string eday = null;
        string shhss = " 00:00";
        string ehhss = " 23:59";
        string smonth = null;
        string emonth = null;
        string groupfield = null;
        try
        {
          
            JObject rules = JObject.Parse(limit.ToString());

            if (null != rules["filter"])
                groupfield = rules["filter"].ToString();

            if (string.IsNullOrEmpty(stime) && string.IsNullOrEmpty(etime))
            {
                string date = DateTime.Now.ToString("yyyy-MM-dd");
                sday = date + shhss;
                eday = date + ehhss;

                DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(-1);
                smonth = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01" + shhss;
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }
            else
            {
                sday = stime;
                eday = etime;

                DateTime dtime = Convert.ToDateTime(stime);
                smonth = dtime.Year.ToString() + "-" + dtime.Month.ToString() + "-01" + shhss;
                DateTime monthDays = new DateTime(dtime.Year, (dtime.Month), 1).AddMonths(1).AddDays(-1);
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }

            if (!string.IsNullOrEmpty(rules["monthfee"].ToString()) && Convert.ToInt32(rules["monthfee"]) > 0)
            {
                int month = Service.getLimitFeeTotal(conduitid, companyid, productid, null, smonth, emonth, groupfield, tabname);
                if (month >= Convert.ToInt32(rules["monthfee"]))
                {
                    rules.Remove("limit");
                    rules.Remove("dayfee");
                    rules.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                    rules.Add(new JProperty("key", conduitid + "m"));
                    info = rules.ToString();
                }
            }
            else if (!string.IsNullOrEmpty(rules["dayfee"].ToString()) && Convert.ToInt32(rules["dayfee"]) > 0)
            {
                int day = Service.getLimitFeeTotal(conduitid, companyid, productid, null, sday, eday, groupfield, tabname);
                if (day >= Convert.ToInt32(rules["dayfee"]))
                {
                    rules.Remove("limit");
                    rules.Remove("monthfee");
                    rules.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                    rules.Add(new JProperty("key", conduitid + "d"));
                    info = rules.ToString();
                }
            }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>LimitRules获取数据异常[" + e.ToString() + "]");
            return info;
        }
        return info;
    }



    /// <summary>
    /// 省份限制
    /// </summary>
    /// <param name="limit"></param>
    /// <param name="area"></param>
    /// <param name="conduitid"></param>
    /// <param name="companyid"></param>
    /// <param name="productid"></param>
    /// <param name="stime"></param>
    /// <param name="etime"></param>
    /// <param name="tabname"></param>
    /// <returns></returns>
    public static string LimitRules(object limit, string area, int conduitid, int companyid, int productid, string stime, string etime, string tabname)
    {
        string info = null;
        string sday = null;
        string eday = null;
        string shhss = " 00:00";
        string ehhss = " 23:59";
        string smonth = null;
        string emonth = null;
        bool distinct = false;
        string countfield = null;

        try
        {

            if (limit.ToString().IndexOf(area) == -1)
                return info;

            JArray rules = JArray.Parse(limit.ToString());

            if (string.IsNullOrEmpty(stime) && string.IsNullOrEmpty(etime))
            {
                string date = DateTime.Now.ToString("yyyy-MM-dd");
                sday = date + shhss;
                eday = date + ehhss;

                DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(-1);
                smonth = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01" + shhss;
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }
            else
            {
                sday = stime;
                eday = etime;

                DateTime dtime = Convert.ToDateTime(stime);
                smonth = dtime.Year.ToString() + "-" + dtime.Month.ToString() + "-01" + shhss;
                DateTime monthDays = new DateTime(dtime.Year, (dtime.Month), 1).AddMonths(1).AddDays(-1);
                emonth = monthDays.ToString("yyyy-MM-dd") + ehhss;
            }

            foreach (JObject items in rules)
            {
                if (items["area"].ToString().IndexOf(area) > -1)
                {
                    if (null!=items["filter"])
                        distinct = Convert.ToBoolean(items["filter"]);
                    if (null != items["field"])
                        countfield = items["field"].ToString();

                    if (Convert.ToInt32(items["month"]) > 0)
                    {
                        int month = Service.getLimitAreaTotal(conduitid, companyid, productid, items["area"].ToString(), smonth, emonth, distinct, countfield, tabname);
                        if (month >= Convert.ToInt32(items["month"]))
                        {
                            items.Remove("day");
                            items.Remove("dayfee");
                            items["stime"] = smonth;
                            items["etime"] = emonth;
                            items.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                            items.Add(new JProperty("key", conduitid + "m"));
                            info = items.ToString();
                            break;
                        }
                    }
                    if (Convert.ToInt32(items["day"]) > 0)
                    {
                        int day = Service.getLimitAreaTotal(conduitid, companyid, productid, items["area"].ToString(), sday, eday, distinct, countfield, tabname);
                        if (day >= Convert.ToInt32(items["day"]))
                        {
                            items.Remove("month");
                            items.Remove("monthfee");
                            items["stime"] = sday;
                            items["etime"] = eday;
                            items.Add(new JProperty("datatime", DateTime.Now.ToString("yyyy-MM-dd")));
                            items.Add(new JProperty("key", conduitid + "d"));
                            info = items.ToString();
                            break;
                        }
                    }

                    break;
                }
            }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "============>LimitRules获取数据异常[" + e.ToString() + "]");
            return info;
        }
        return info;
    }

    public static string getOrderProductCode(string userOrder,string tabname)
    {
        string info =null;

        string sql = "select top 1 productCode from " + tabname + " WITH(NOLOCK) where datatime between dateadd(day,-2,GETDATE()) and dateadd(day,1,GETDATE()) and userorder='" + userOrder + "' ";
        try
        {
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
                info = dt.Rows[0]["productCode"].ToString();
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getOrderProductCode获取数据异常[" + e.ToString() + "]");
        }
        return info;
    }

    

    /// <summary>
    /// 
    /// </summary>
    /// <param name="conduitid"></param>
    /// <param name="companyid"></param>
    /// <param name="productcode"></param>
    /// <param name="tabname"></param>
    /// <param name="rules"></param>
    /// <returns></returns>
    public static bool getCountRequest(int conduitid, int companyid, string productcode, string tabname, string rules)
    {
        bool flag = false;
        string param = string.Empty;
        if (!string.IsNullOrEmpty(productcode))
            param = " and productcode='" + productcode + "'";

        string sql = "select count(mobile) from " + tabname + " WITH(NOLOCK) where conduitid=" + conduitid + " and companyid=" + companyid + param;
        try
        {
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
            {
               // int info = Convert.ToInt32(old);
               // if (info >= number)
                //    flag = true;
            }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>getCountRequest获取数据异常[" + e.ToString() + "]");
        }

        return flag;
    }

    /// <summary>
    /// 设置上行数据参数(通过查询数据表来获取数据)
    /// </summary>
    /// <param name="field"></param>
    /// <param name="tabname"></param>
    /// <param name="conduitid"></param>
    /// <param name="companyid"></param>
    /// <param name="productcode"></param>
    /// <returns></returns>
    public static DataTable setUpInterfaceData(string field, string tabname, int conduitid, int companyid, string productcode)
    {
        DataTable dt = new DataTable();
        string param="";

        if(conduitid>0)
            param+=" and conduitid=" + conduitid;

        if (companyid > 0)
            param += " and companyid=" + companyid;

        if (!string.IsNullOrEmpty(productcode))
            param += " and productcode='" + productcode+"'";

        string sql = "select " + field + " from " + tabname + " WITH(NOLOCK) where infoid>0" + param;
        try
        {
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>setUpInterfaceData获取数据异常[" + e.ToString() + "]");
        }

        return dt;
    }


    /// <summary>
    /// 获取查询指定字段
    /// </summary>
    /// <param name="field"></param>
    /// <param name="tabname"></param>
    /// <param name="param"></param>
    /// <returns></returns>
    public static DataTable setUpInterfaceData(string field, string tabname, string param)
    {
        DataTable dt = new DataTable();

        string sql = "select " + field + " from " + tabname + " WITH(NOLOCK) where "+ param;
        try
        {
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), sql + "============>setUpInterfaceData获取数据异常[" + e.ToString() + "]");
        }

        return dt;
    }


    public static bool setUpInterfaceData(string sqlstr, string sqlparam, int conduitid, DataTable settab)
    {
        bool flag = true;
        string sql = null;
        string debug = null;

        DataTable dt = new DataTable();
        
        try
        {
            if (!string.IsNullOrEmpty(sqlparam))
            {
                settab.CaseSensitive = false;

                string[] p = sqlparam.Split(',');
                object[] param = new object[p.Length + 1];
                param[0] = conduitid;
                for (int i = 0; i < p.Length; i++)
                {
                    DataRow[] item = settab.Select("inField='" + p[i] + "'");//查询
                    if (item.Length > 0)
                        param[i + 1] = item[0]["valuedata"].ToString();
                }
                sql = string.Format(sqlstr, param);

                debug = JsonConvert.SerializeObject(settab, new DataTableConverter());
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                foreach (DataRow dr in settab.Rows)
                {
                    string name = dr["inField"].ToString();
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        string value = dt.Columns[i].ColumnName;
                        if (name.ToLower() == value.ToLower())
                            dr["valuedata"] = dt.Rows[0][value].ToString();
                    }
                }
            }
        }
        catch (Exception e)
        {
            LogHelper.WriteLog(typeof(Service), "==========setUpInterfaceData(string sqlstr, string sqlparam, int conduitid, DataTable settab)获取数据时异常==========>\r\n"
                                                              + "sqlparam:[" + sqlparam + "]\r\n"
                                                              + "sqlstr:[" + sqlstr + "]\r\n"
                                                              + "conduitid:[" + conduitid + "]\r\n"
                                                              + "settab:" + debug + "\r\n"
                                                              + "[" + e.ToString() + "]\r\n"
                                             + "===========================================END==============================================\r\n");
            return false;
        }

        return flag;
    }


    


    }//
}
