using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

using app.Common;
using app.Entity;
using app.Data;


namespace app.Service
{
    public class FromCPGw
    {

        public static string getSmsdatajson(string info)
        {
            int olid = 0;
            int resultCode=0;
            string data = null;
            JObject jsonObj = new JObject();
            try
            {
                JObject json = JObject.Parse(info);
                string streamingNo=json["body"]["streamingNo"].ToString();
                string productId = json["body"]["productId"].ToString();
                string featureStr=json["body"]["featureStr"].ToString();
                string result = Service.getFeatureStrx(productId);
             
                if(null==result)
                     resultCode=1;
                else{
                   JObject code=JObject.Parse(result);
                   if(code["ordered"].ToString()!=featureStr)
                     resultCode=2;  
                };
                JObject reinfo = new JObject(
                                    new JProperty("result",
                                           new JObject(new JProperty("streamingNo", streamingNo)),
                                           new JObject(new JProperty("resultCode", resultCode))
                                   )
                              );
                data = "{"+reinfo["result"].ToString()+"}";
                jsonObj.Add(json);
                string sql = "INSERT INTO [tab_2016](streamingNo,productId,smsdatajsonpacket,message) VALUES('" + streamingNo + "','" + productId + "','" + jsonObj.ToString() + "','" + featureStr + "')";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                
                if (olid < 1)
                {
                    LogHelper.WriteLog(typeof(FromCPGw), "============>数据插入失败[" + sql + "]");
                    return null;
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), JsonConvert.SerializeObject(info) + "============>数据插入异常[" + e.ToString() + "]");
            }

            return data;

        }

        /// <summary>
        /// 点播业务
        /// 向签约用户发送短消息(下行)
        /// </summary>
        /// <returns></returns>
        public static string sendSms(string info, string correlator)
        {
            string data=null;
            int olid = 0;
             try
            {
                JObject jsonObj = new JObject();
                string sendSms = Service.getFormant("sendSms", System.Text.Encoding.UTF8);
                string proNo=DateTime.Now.ToString("yyyyMMddHHmmssfff");
                JObject downpack =JObject.Parse(sendSms);
                JObject uppack= JObject.Parse(info);
                string codeinfo=Service.getProductInfo(uppack["body"]["message"]["message"].ToString());
                /*if (null == codeinfo)
                {
                    string status = Service.getFormantZH("errorCode", System.Text.Encoding.UTF8);
                    JObject error=JObject.Parse(status);
                    string upsql = "UPDATE [tab_2016] SET status='" + error["11"] + "',resultCode='11' WHERE linkID='" + uppack["header"]["linkId"].ToString() + "'";
                    olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, upsql);
                    return null;

                }*/
                JArray jsondata = JArray.Parse(codeinfo.ToLower());
                downpack["body"]["addresses"][0] = uppack["body"]["message"]["senderAddress"].ToString();
                downpack["body"]["senderName"] = uppack["body"]["message"]["smsServiceActivationNumber"].ToString();
                downpack["body"]["message"] = jsondata[0]["omessage"].ToString();
                downpack["body"]["receiptRequest"]["endpoint"]=uppack["body"]["message"]["senderAddress"].ToString();
                downpack["body"]["receiptRequest"]["interfaceName"]="sendSms";
                //string correlator = GUID.CreatGUID("N").Substring(0, 20).ToUpper();
                downpack["body"]["receiptRequest"]["correlator"] = correlator;
	            downpack["body"]["charging"]["amount"]=0;
	            downpack["header"]["cpId"]=ConfigurationManager.AppSettings["cpId"];
                downpack["header"]["cpPassword"]=ConfigurationManager.AppSettings["cpPassword"];
                downpack["header"]["timeStamp"]=DateTime.Now.ToString("MMddHHmmss");
                downpack["header"]["productId"] = jsondata[0]["productcode"].ToString();
                downpack["header"]["linkID"]=uppack["header"]["linkId"].ToString();
                downpack["header"]["oa"] = uppack["body"]["message"]["senderAddress"].ToString();
                jsonObj.Add("body",downpack["body"]);
                jsonObj.Add("header",downpack["header"]);
                data = Utils.ClearBR(jsonObj.ToString()).Replace(" ","");

                /*string sql = "UPDATE [tab_2016] SET downpacket='" + data.Replace(" ", "") + "',productcode='" + jsondata[0]["productcode"] + "',correlator='" + correlator + "' WHERE linkID='" + uppack["header"]["linkId"].ToString() + "'";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                {
                    LogHelper.WriteLog(typeof(FromCPGw), "============>sendSms数据更新失败[" + sql + "]");
                    return null;
                }*/
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), info + "============>sendSms数据更新异常[" + e.ToString() + "]");
                
            }
            return data;
        }


        /// <summary>
        /// 点播业务
        /// 将得到的网关下行标识更新到数据库
        /// </summary>
        /// <param name="info"></param>
        /// <param name="data"></param>
        public static void sendSmsResponse(string info, string data)
        {
            int olid = 0;
            try
            {
                JObject downpack = JObject.Parse(data);
                string sql = "UPDATE [tab_2016] SET RequestIdentifier='" + info + "' WHERE linkID='" + downpack["header"]["linkID"].ToString() + "'";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid <1)
                    LogHelper.WriteLog(typeof(FromCPGw), "============>sendSmsResponse数据更新失败[" + sql + "]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), "sendSmsResponse============>数据更新异常[" + e.ToString() + "]");
            }
        }


        /// <summary>
        /// 点播业务
        /// 接收网关的上行信息
        /// </summary>
        /// <param name="info"></param>
        public static int notifySmsReception(string info ,ref string correlator)
        {
            int olid = 0;
           
            try
            {
                correlator = GUID.CreatGUID("N").Substring(0, 20).ToUpper();
                JObject jsonObj = JObject.Parse(info);
           
                string mobile = jsonObj["body"]["message"]["senderAddress"].ToString();
                string message= jsonObj["body"]["message"]["message"].ToString();
                string san = jsonObj["body"]["message"]["smsServiceActivationNumber"].ToString();
                string linkid = jsonObj["header"]["linkId"].ToString();
                mobileArea area = Service.getMobileArea(mobile);//获取地区
                string data = Service.getProductInfo(message);//获取公司及指令信息
                if (null != data)
                {
                    JArray items = JArray.Parse(data);
                    string sql = "INSERT INTO [tab_2016](linkID,Mobile,upPacket,message,san,area,productID,companyID,buyID,orderresult,fee,buyname,correlator,productcode) VALUES('" + linkid + "','" + mobile + "','" + info + "','" + message + "','" + san + "','" + area.Area + "'," + items[0]["productID"] + "," + items[0]["companyID"] + "," + items[0]["buyID"] + ",'定购'," + items[0]["price"] + ",'" + items[0]["buyname"] + "','" + correlator + "','" + items[0]["ProductCode"] + "')";

                    olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                    if (olid < 1)
                        LogHelper.WriteLog(typeof(FromCPGw), "============>notifySmsReception数据查入失败[" + sql + "]");
                }
                else
                    LogHelper.WriteLog(typeof(FromCPGw), info + "============>notifySmsReception[无效指令数据]");
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), info + "============>notifySmsReception数据插入异常[" + e.ToString() + "]");
                return olid;
            }
            return olid;
        }

        /// <summary>
        /// 点播业务
        /// 得到短信状态报告,并更新数据库
        /// </summary>
        /// <param name="info"></param>
        public static int notifySmsDeliveryReception(string info, ref string correlator)
        {
            int olid = 0;
            string resultCode = "0";
            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    JObject jsonObj = JObject.Parse(info);
                    string status = Service.getFormantZH("DeliveryStatus", System.Text.Encoding.UTF8);
                    JObject pack =JObject.Parse(status);
                    string item = jsonObj["body"]["deliveryStatus"]["deliveryStatus"].ToString();
                    correlator = jsonObj["body"]["correlator"].ToString();
                    if (item != "DeliveredToTerminal")
                        resultCode = "99";
                    string sql = "UPDATE [tab_2016] SET status='" + pack[item].ToString() + "',resultCode='" + resultCode + "' WHERE correlator='" + correlator + "'";

                    olid = SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql);
                    trans.Commit();
                    if (olid < 1)
                        LogHelper.WriteLog(typeof(FromCPGw), "============>notifySmsDeliveryReception数据更新失败[" + sql + "]");
                       
                }
                catch (Exception e)
                {
                    trans.Rollback();
                   
                    LogHelper.WriteLog(typeof(FromCPGw), info+"============>notifySmsDeliveryReception异常[" + e.ToString() + "]");
                }
                finally
                {
                    conn.Close();
                }
            }

            return olid;
        }

        /// <summary>
        /// 包月
        /// 订购成功通知接口
        /// </summary>
        /// <param name="info"></param>
        public static string orderSuccessNotify(string info)
        {
           
            int olid = 0;
            string data = null;
            string correlator = null;

            try
            {
                correlator = GUID.CreatGUID("N").ToUpper();
                data = Service.getFormant("returnCode", System.Text.Encoding.UTF8);
                string status = Service.getFormantZH("resultCode", System.Text.Encoding.UTF8);

                JObject result = JObject.Parse(status);
                JObject datajson = JObject.Parse(data);
                
                //"{"body":{"streamingNo":"2016091410575213500000000000024306118911883406","userId":"18911883406","productId":"135000000000000243061","productAccessNo":"1065987899928330","featureStr":"30","timeStamp":"2016-09-14 10:57:52"}}"
                
                JObject jsonObj = JObject.Parse(info);
               
                mobileArea area = Service.getMobileArea(jsonObj["body"]["userId"].ToString());//查找省份
                string proinfo = Service.getProductInfo(jsonObj["body"]["featureStr"].ToString());//获取公司及指令信息
                JArray items= JArray.Parse(proinfo);

                string sql = "INSERT INTO [tab_2016](StreamingNo,Mobile,successnotifypacket,message,area,productID,companyID,buyID,orderresult,fee,buyname,correlator,productcode,status,resultCode) "
                    + "VALUES('" + jsonObj["body"]["streamingNo"].ToString() + "','" + jsonObj["body"]["userId"].ToString() + "','" + info + "','" + jsonObj["body"]["featureStr"].ToString() + "','" + area.Area + "'," + items[0]["productID"] + "," + items[0]["companyID"] 
                    + "," + items[0]["buyID"] + ",'定购'," + items[0]["price"] + ",'" + items[0]["buyname"] + "','" + correlator + "','" + items[0]["ProductCode"] + "','通知','" + datajson["resultCode"].ToString() + "')";
;
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 0)
                    LogHelper.WriteLog(typeof(FromCPGw), "============>orderSuccessNotify数据更新失败[" + sql + "]");

            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), info+ "orderSuccessNotify============>异常[" + e.ToString() + "]");
                return correlator;
            }

            return correlator;

        }



        /// <summary>
        /// 包月
        /// CpGw获取用户业务执行请求后，发送该消息，Cp根据业务情况，决定下发时间
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string serviceConsumenotifyReq(string info)
        {
            int olid = 0;
            string data =null;
            try
            {
                data = Service.getFormant("returnCode", System.Text.Encoding.UTF8);
                JObject jsonObj = JObject.Parse(info);
               
                JObject serviceresult = JObject.Parse(Service.getFormantZH("resultCode", System.Text.Encoding.UTF8));
                JObject datajson = JObject.Parse(data);
                datajson["streamingNo"] = jsonObj["body"]["streamingNo"].ToString();
                jsonObj.Add(new JProperty("result", datajson));

                string sql = "INSERT INTO [tab_2016](linkid,StreamingNo,mobile,status,serviceNotifypacket,productCode,resultCode,message) VALUES('" + jsonObj["body"]["linkId"].ToString() + "','" + jsonObj["body"]["streamingNo"].ToString() + "','" + jsonObj["body"]["userId"].ToString() + "','" + serviceresult["0"].ToString() + "','" + jsonObj.ToString() + "','" + jsonObj["body"]["productId"].ToString() + "','" + datajson["resultCode"].ToString() + "','" + jsonObj["body"]["featureStr"].ToString() + "')";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid <1)
                    LogHelper.WriteLog(typeof(FromCPGw), "============>serviceConsumenotifyReq数据添加失败[" + sql + "]");

                data = datajson.ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), info + "============>serviceConsumenotifyReq异常[" + e.ToString() + "]");
                return null;
            }

            return data;

        }

        /// <summary>
        /// 包月上行
        /// 订购\退订产品，cpGw调用本接口
        /// </summary>
        /// <returns></returns>
        public static string orderRelationUpdateNotifyReq(string info, ref string correlator)
        {
            int olid = 0;
            string sql = null;
            string data = null;
            string temp=null;
 
            try
            {
                data = Service.getFormant("returnCode", System.Text.Encoding.UTF8);
                string status = Service.getFormantZH("OPType", System.Text.Encoding.UTF8);
                JObject orderresult = JObject.Parse(Service.getFormantZH("resultCode", System.Text.Encoding.UTF8));
                JObject datajson = JObject.Parse(data);
                JObject jsonObj = JObject.Parse(info);
                JObject pack =JObject.Parse(status);
                string result = jsonObj["body"]["opType"].ToString();
               
                jsonObj.Add(new JProperty("result", datajson));
                datajson["streamingNo"] = jsonObj["body"]["streamingNo"].ToString();
                string codeinfo = Service.getFeatureStrx(jsonObj["body"]["productId"].ToString());//查找产品
                mobileArea area = Service.getMobileArea(jsonObj["body"]["userId"].ToString());//查找省份
                correlator = GUID.CreatGUID("N").ToUpper();
                if (null == codeinfo)
                {
                    datajson["resultCode"] = 14;
                    jsonObj["result"]["resultCode"] = 14;

                    sql = "INSERT INTO [tab_2016](StreamingNo,mobile,status,UpdateNotifypacket,productCode,resultCode,correlator) "
                           + "VALUES('" + jsonObj["body"]["streamingNo"].ToString() + "','" + jsonObj["body"]["userId"].ToString() + "','" + orderresult[datajson["resultCode"].ToString()].ToString() + "','" + Utils.ClearBR(jsonObj.ToString()).Replace(" ", "") + "','" + jsonObj["body"]["productId"].ToString() + "','" + datajson["resultCode"].ToString() + "','" + correlator + "')";
                }
                else
                {
                    JObject jsondata = JObject.Parse(codeinfo);
                    temp = jsonObj["body"]["streamingNo"].ToString();
                    string code = jsondata["ordered"].ToString();
                    if (result == "3")
                        code = jsondata["unsubscribe"].ToString();

                    sql = "INSERT INTO [tab_2016](message,companyid,StreamingNo,mobile,status,UpdateNotifypacket,productCode,orderresult,resultCode,area,productID,buyID,correlator,fee,buyname) "
                          + "VALUES('" + code + "'," + jsondata["companyID"] + ",'" + jsonObj["body"]["streamingNo"].ToString() + "','" + jsonObj["body"]["userId"].ToString() + "','" + orderresult[datajson["resultCode"].ToString()].ToString() + "'" 
                          + ",'" + Utils.ClearBR(jsonObj.ToString()).Replace(" ", "") + "','" + jsonObj["body"]["productId"].ToString() + "','" + pack[result].ToString() + "','" + datajson["resultCode"].ToString() + "'"
                          + ",'" + area.Area + "'," + jsondata["productID"] + ",'" + jsondata["buyID"] + "','" + correlator + "'," + jsondata["price"] + ",'" + jsondata["buyname"] + "')";
                }
               olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
               if (olid <1)
                 LogHelper.WriteLog(typeof(FromCPGw), "============>orderRelationUpdateNotifyReq数据添加失败[" + sql + "]");
               
                data =datajson.ToString();
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(FromCPGw), info + "============>orderRelationUpdateNotifyReq异常[" + e.ToString() + "]");
                return null;
            }

            return data;
        }
 
    }
}
