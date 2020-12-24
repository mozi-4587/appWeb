using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

using app.Common;
using app.Entity;
using app.Data;


namespace app.Manage
{
    public class baseManage
    {
        public static string getJsonSqlparams(string data)
        {
            string param = "";
            JObject o = JObject.Parse(data);
            IEnumerable<JProperty> properties = o.Properties();
            foreach (JProperty item in properties)
                param += "," + item.Name + "='" + item.Value + "'";

            return param.Substring(1);
        }



        /// <summary>
        /// 添加下游公司信息
        /// </summary>
        /// <param name="info"></param>
        public static string setCompanyInfo(int infoid,string data)
        {
            string field=string.Empty;
            string value=string.Empty;
            string setfield = string.Empty;
            JObject o = JObject.Parse(data);
           
            IEnumerable<JProperty> properties = o.Properties();
            foreach (JProperty item in properties)
            {
                field+=","+item.Name;
                value+=",'"+item.Value+"'";
                setfield += "," + item.Name + "='" + item.Value + "'";
            }
            string sql = "IF EXISTS(SELECT infoid FROM company where infoid=" + infoid + ")"
                         + " UPDATE company SET " + setfield.Substring(1) + " where infoid=" + infoid
                         + " ELSE INSERT INTO company(" + field.Substring(1) + ") VALUES(" + value.Substring(1) + ")";
            int old = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (old>-1)
               return "设置完成";
            else
               return "设置失败";
        }



        public static int addActionInfo(string data, int productid)
        {
            
            JArray info=JArray.Parse(data);
            string idlist = "";
            int id=0;
           

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    if (info[0]["sole"].ToString() == "1")
                        id = SqlHelper.ExecuteNonQuery(trans, CommandType.Text, "update action set companyid=" + info[0]["infoid"].ToString() + ",syncUrl='" + info[0]["syncUrl"].ToString() + "',point=" + info[0]["point"].ToString() + ",divideInto=" + info[0]["divideInto"].ToString() + " where productid=" + productid);
                    else
                    {
                        foreach (JObject item in info)
                        {
                            string field = "";
                            string value = "";

                            idlist += "," + item["infoid"].ToString();
                            /* string productcode=null;
                             string ordered=null;
                             string unsubscribe=null;*/


                            if (!string.IsNullOrEmpty(item["productcode"].ToString()))
                            {
                                field += ",productcode";
                                value += ",'" + item["productcode"].ToString() + "' as productcode";
                            }
                            else
                            {
                                if (!string.IsNullOrEmpty(item["ordered"].ToString()))
                                {
                                    field += ",ordered";
                                    value += ",'" + item["ordered"].ToString() + "' as ordered";
                                }
                                if (!string.IsNullOrEmpty(item["unsubscribe"].ToString()))
                                {
                                    field += ",unsubscribe";
                                    value += ",'" + item["unsubscribe"].ToString() + "' as unsubscribe";
                                }
                            }

                            string sql = "insert into action(companyid,syncStart,conduitid,productid,price,point,divideInto,syncUrl,syncFlag,syncMethod,operators,buyid" + field + ") "
                                + "select '" + item["infoid"].ToString() + "' as companyid ,'" + item["syncStart"].ToString() + "' as syncStart,'" + item["conduitid"].ToString() + "' as conduitid, infoid,'" + item["price"].ToString() + "' as price,'" + item["point"].ToString() + "' as point,'" + item["divideInto"].ToString() + "' as divideInto,'" + item["syncUrl"].ToString() + "' as syncUrl"
                                + ",'" + item["syncFlag"].ToString() + "' as syncFlag,'" + item["syncMethod"].ToString() + "' as syncMethod,operator,buy" + value + " from product where infoid=" + productid;

                            id = SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql);
                        }
                        if (string.IsNullOrEmpty(info[0]["ordered"].ToString()) && string.IsNullOrEmpty(info[0]["productcode"].ToString()))
                        {
                            string sql = "update action set productcode=b.infoid from action as b where b.productid=" + productid + " and b.companyid in(" + idlist.Substring(1) + ") and b.infoid=infoid";
                            id = SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql);
                        }
                    }
                    
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    return -1;
                    // throw ex;
                }
                finally
                {
                    conn.Close();
                }

            }
            return 0;

            
        }

        /// <summary>
        /// 删除下游公司信息
        /// </summary>
        /// <param name="info"></param>
        public static string delCompanyInfo(string infoid)
        {
            return "";
        }



        public string addBlackList(DataTable dataTable, int batchSize)
        {

            if (dataTable.Rows.Count == 0)
            {
                return "";
            }
            using (SqlConnection connection = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction))
            {
                try
                {
                    connection.Open();
                    //给表名加上前后导符
                    var tableName = "balcklist";
                    using (var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, null)
                    {
                        DestinationTableName = tableName,
                        BatchSize = batchSize
                    })
                    {

                        bulk.WriteToServer(dataTable);
                        bulk.Close();
                    }
                }
                catch (Exception ex)
                {
                    return "";
                }
                finally
                {
                    connection.Close();
                }

            }

            return "";
        }


        /// <summary>
        /// 获取表结构
        /// </summary>
        /// <param name="tabname"></param>
        /// <returns></returns>
        public static string getTabItems(string tabname)
        {
            string info = null;


            return info;
        }


        /// <summary>
        /// 获取表字段
        /// </summary>
        /// <param name="tabname"></param>
        /// <returns></returns>
        public static string getSysColumns(string tabname)
        {
            string info = null;
            if (null != tabname)
            {
                string sql = "select b.title,a.name,a.user_type_id,b.sysmust from sys.columns as a left join public_sysfield as b on b.sysfield=a.name where a.object_id=OBJECT_ID('" + tabname + "') and b.tablename='" + tabname + "'";
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt).Replace("[", "").Replace("]", "");
            }

            return info;
        }


        /// <summary>
        /// 设置省份及规则
        /// </summary>
        /// <param name="data"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        public static string setRuleArea(string data, string node)
        {
            string message=string.Empty;
            string sql1 = null;
            string sql2 = null;
            string sql3 = null;

            DataTable proinfo = new DataTable();
            JObject nodejson=JObject.Parse(node);
            JObject areainfo = JObject.Parse("{'enable':'','disabled':'','get':false,'message':'屏蔽省份'}");
            JObject rulearea = JObject.Parse("{'area':'','day':'','month':'','city':'','stime':'','etime':''}");
         
            JArray rulelist=new JArray();
            JArray list = JArray.Parse(data);

            string enable = string.Empty;
            string disabled = string.Empty;

            foreach (JObject item in list)
            {
                
                if (Convert.ToBoolean(item["allow"]) == false)
                    disabled += "," + item["name"].ToString();
                else
                    enable += "," + item["name"].ToString();

                if(!string.IsNullOrEmpty(item["day"].ToString()))
                {
                    rulearea["area"] = item["name"].ToString();
                    rulearea["day"] = item["day"].ToString();
                    rulearea["month"] = item["month"].ToString();
                    rulelist.Add(rulearea);
                }
            }
            areainfo["enable"] = enable.Substring(1);
            //if(!string.IsNullOrEmpty(disabled))
                //areainfo["disabled"] = disabled.Substring(1);

            if (Convert.ToInt32(nodejson["level"]) == 1)
            {

                sql1 = "insert into rules(limit,productid,typeid) values('" + areainfo.ToString().Replace(" ", "") + "'," + nodejson["infoid"].ToString() + ",6)";//开通
                sql2 = "insert into rules(limit,productid,typeid) values('" + rulelist.ToString().Replace(" ", "") + "'," + nodejson["infoid"].ToString() + ",2)";//单省
                sql3 = "update rules set limit='" + areainfo.ToString().Replace(" ", "") + "' where infoid=" + nodejson["infoid"].ToString();
               
                proinfo = dataManage.getProductRules(Convert.ToInt32(nodejson["infoid"]), 0, null);
                if (proinfo.Rows.Count > 0)
                {
                    //sql3 = null;
                    DataRow[] ruleinfo = proinfo.Select("typeid=6");
                    DataRow[] arearule = proinfo.Select("typeid=2");
                   
                    if (ruleinfo.Length > 0)
                        sql1 = "update rules set limit='" + areainfo.ToString().Replace(" ", "") + "' where productid=" + ruleinfo[0]["productid"].ToString() + " and typeid=6";

                    if (arearule.Length > 0)
                        sql2 = "update rules set limit='" + rulelist.ToString().Replace(" ", "") + "' where productid=" + ruleinfo[0]["productid"].ToString() + " and typeid=2";

                }
            }
            else if (Convert.ToInt32(nodejson["level"]) == 2)
            {
               
                sql1 = "insert into rules(limit,productid,actionid,typeid) values('" + areainfo.ToString() + "'," + nodejson["pid"].ToString() + "," + nodejson["actionid"].ToString() + ",6)";
                sql2 = "insert into rules(limit,productid,actionid,typeid) values('" + rulelist.ToString() + "'," + nodejson["pid"].ToString() + "," + nodejson["actionid"].ToString() + ",2)";
                //sql3 = "update action set ruleflag=1 where infoid=" + nodejson["actionid"].ToString();
                proinfo = dataManage.getProductRules(Convert.ToInt32(nodejson["pid"]), Convert.ToInt32(nodejson["actionid"]), null);
                if (proinfo.Rows.Count > 0)
                {
                    //sql3 = null;
                    DataRow[] ruleinfo = proinfo.Select("actionid=" + nodejson["actionid"].ToString() + " and typeid=6");
                    DataRow[] arearule = proinfo.Select("actionid=" + nodejson["actionid"].ToString() + " and typeid=2");

                    if (ruleinfo.Length > 0)
                        sql1 = "update rules set limit='" + areainfo.ToString().Replace(" ", "") + "' where infoid=" + ruleinfo[0]["infoid"].ToString();

                    if (arearule.Length > 0)
                        sql2 = "update rules set limit='" + rulelist.ToString().Replace(" ", "") + "' where infoid=" + ruleinfo[0]["infoid"].ToString();
                }
            }
            else
                return "无法设置开通省份!";

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    int id2 = 0;
                    int id3 = 0;

                    if(!string.IsNullOrEmpty(sql3))
                     id3 = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql3), 0);

                    int id1 = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql1), 0);

                    if (rulelist.Count > 0)
                        id2 = Convert.ToInt32(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql2));


                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    return "设置失败!";
                    // throw ex;
                }
                finally
                {
                    conn.Close();
                }
               
            }
            return "设置完成!";
        }



        public static string setRuleArea(string data, string rule, int productid, int companyid,int fee)
        {
            string message = string.Empty;
            string enable = string.Empty;
            string disabled = string.Empty;

            string sql1 = null;
            string sql2 = null;
            string sql3 = null;

            int infoid2=0;
            int infoid6=0;

            DataTable proinfo = new DataTable();
           
            JObject areainfo = JObject.Parse("{'enable':'','disabled':'','get':false,'message':'屏蔽省份'}");
            
            
            JArray rulelist = new JArray();
            JArray list = JArray.Parse(data);

            JObject info=JObject.Parse(rule);

            info.Remove("setmode");

            if (null != info["2"])
            {
                infoid2 = Convert.ToInt32(info["2"]);
                info.Remove("2");
            }

            if (null != info["6"])
            {
                infoid6 = Convert.ToInt32(info["6"]);
                info.Remove("6");
            }

            foreach (JObject item in list)
            {
                if (Convert.ToBoolean(item["allow"]) == false)
                    disabled += "," + item["name"].ToString();
                else
                    enable += "," + item["name"].ToString();

                if (!string.IsNullOrEmpty(item["day"].ToString()))
                {
                    JObject rulearea = JObject.Parse("{'area':'','day':'','month':'','city':'','stime':'','etime':''}");
                    rulearea["area"] = item["name"].ToString();
                    rulearea["day"] = item["day"].ToString();
                    if (!string.IsNullOrEmpty(item["month"].ToString()))
                       rulearea["month"] = item["month"].ToString();
                    else
                       rulearea["month"] = 0;
                    rulelist.Add(rulearea);
                }
            }

            areainfo["enable"] = enable.Substring(1);


            if (fee > 0)
            {
                sql1 = "insert into rules(limit,productid,typeid,companyid,setfee) values('" + areainfo.ToString().Replace(" ", "") + "'," + productid + ","+companyid+","+fee+",6)";//开通省份

                if (rulelist.Count > 0)
                {
                    info.Add(new JProperty("data", rulelist));
                    sql2 = "insert into rules(limit,productid,typeid,companyid,setfee) values('" + info.ToString().Replace(" ","") + "'," + productid + "," + companyid + "," + fee + ",2)";//单省限量规则
                }
            }
            else if (companyid > 0 && fee == 0)
            {
                sql1 = "insert into rules(limit,productid,typeid,companyid) values('" + areainfo.ToString().Replace(" ", "") + "'," + productid + "," + companyid + ",6)";//开通省份

                if (rulelist.Count > 0)
                {
                    info.Add(new JProperty("data", rulelist));
                    sql2 = "insert into rules(limit,productid,typeid,companyid) values('" + info.ToString().Replace(" ", "") + "'," + productid + "," + companyid + ",2)";//单省限量规则
                }
            }
            else
            {
                sql1 = "insert into rules(limit,productid,typeid) values('" + areainfo.ToString().Replace(" ", "") + "'," + productid +",6)";//开通省份

                if (rulelist.Count > 0)
                {
                    info.Add(new JProperty("data", rulelist));
                    sql2 = "insert into rules(limit,productid,typeid) values('" + info.ToString().Replace(" ", "") + "'," + productid + ",2)";//单省限量规则
                }
            } 
            if (infoid6 > 0)
               sql1 = "update rules set limit='" + areainfo.ToString().Replace(" ", "") + "' where infoid=" + infoid6;//单省限量规则
            if (infoid2 > 0)
               sql2 = "update rules set limit='" + info.ToString().Replace(" ", "") + "' where infoid=" + infoid2;//开通省份;

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    if (!string.IsNullOrEmpty(sql1))
                    {
                        int id1 = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql1), 0);
                    }
                    if (!string.IsNullOrEmpty(sql2))
                    {
                        int id2 = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql2), 0);
                    }

                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    return "设置失败!";
                    // throw ex;
                }
                finally
                {
                    conn.Close();
                }

            }
            return "设置完成!";
        }


        /// <summary>
        /// 设置省份规则
        /// </summary>
        /// <param name="companyid"></param>
        /// <param name="data"></param>
        /// <param name="fee"></param>
        /// <param name="infoid"></param>
        /// <param name="productid"></param>
        /// <param name="rule"></param>
        /// <returns></returns>
        public static string setRuleArea(string data, string rule, int productid, int companyid, int fee, int infoid)
        {
            string message = string.Empty;
            string enable = string.Empty;
            string disabled = string.Empty;

            string sql = null;

            DataTable proinfo = new DataTable();

            JArray rulelist = new JArray();
            JArray list = JArray.Parse(data);

            JObject info = JObject.Parse(rule);

            info.Remove("setmode");

            foreach (JObject item in list)
            {
                if (!string.IsNullOrEmpty(item["day"].ToString()))
                {
                    JObject rulearea = JObject.Parse("{'area':'','day':'','month':'','city':'','stime':'','etime':''}");
                    rulearea["area"] = item["name"].ToString();
                    rulearea["day"] = item["day"].ToString();
                    if (!string.IsNullOrEmpty(item["month"].ToString()))
                        rulearea["month"] = item["month"].ToString();
                    else
                        rulearea["month"] = 0;
                    rulelist.Add(rulearea);
                }
            }

            if (fee > 0)
            {
                if (rulelist.Count > 0)
                {
                    info.Add(new JProperty("data", rulelist));
                    sql = "insert into rules(limit,productid,typeid,companyid,setfee) values('" + info.ToString().Replace(" ", "") + "'," + productid + "," + companyid + "," + fee + ",2)";//单省限量规则
                }
            }
            else if (companyid > 0 && fee == 0)
            {
                if (rulelist.Count > 0)
                {
                    info.Add(new JProperty("data", rulelist));
                    sql = "insert into rules(limit,productid,typeid,companyid) values('" + info.ToString().Replace(" ", "") + "'," + productid + "," + companyid + ",2)";//单省限量规则
                }
            }
            else
            {
                if (rulelist.Count > 0)
                {
                    info.Add(new JProperty("data", rulelist));
                    sql = "insert into rules(limit,productid,typeid) values('" + info.ToString().Replace(" ", "") + "'," + productid + ",2)";//单省限量规则
                }
            }
            if (infoid > 0)
                sql = "update rules set limit='" + info.ToString().Replace(" ", "") + "' where infoid=" + infoid;//单省限量规则;
            
            try
            {
                int id = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception ex)
            {
                return "设置失败!";
                //throw ex;
            }
        
            return "设置完成!";
        }

        /// <summary>
        /// 取消省份及规则
        /// </summary>
        /// <param name="node"></param>
        /// <param name="typeid"></param>
        /// <returns></returns>
        public static string cancelRuleArea(string data, int typeid, int ruleid)
        {

            string sql=null;
            int id = 0;
            DataTable dt=dataManage.getProductRules(ruleid);
            
            if (dt.Rows.Count > 0)
            {
                JObject info = JObject.Parse(dt.Rows[0]["limit"].ToString());
                JArray jarray = JArray.Parse(info["data"].ToString());
                string[] area=data.Split(',');
                int[] ls = new int[area.Length];
                int j = 0;

                for(int i=jarray.Count;i>0;i--)
                {
                    if (data.IndexOf(jarray[i - 1]["area"].ToString()) > -1)
                        ls[j++] = i - 1;
                }

                for (int n = 0; n < ls.Length; n++)
                    jarray.RemoveAt(ls[n]);

                
                if (jarray.Count > 0)
                {
                    info["data"] = jarray;
                    sql = "Update rules set limit='" + info.ToString().Replace(" ", "") + "' where infoid=" + ruleid + " and typeid=" + typeid;
                }
                else
                    sql = "delete rules where infoid=" + ruleid + " and typeid=" + typeid;

                try
                {
                   id = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
                catch (Exception ex)
                {
                    return "{\"code\":" + id + "}";
                    // throw ex;
                }
            }
            return "{\"code\":" + id + "}";
        }


        /// <summary>
        /// 设置同步
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="url"></param>
        /// <returns></returns>
        public static string setSyncUrl(int infoid, string url)
        {
            string sql = string.Format("update action set syncUrl='{0}' where infoid={1}",url.Trim(),infoid);
            
            try
            {
                if (!string.IsNullOrEmpty(url))
                {
                    int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
                
            }
            catch (Exception e)
            {

                return "设置失败!";
            }

            return "设置完成!";
        }

        /// <summary>
        /// 设置扣费
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="point"></param>
        /// <returns></returns>
        public static string setPoint(int infoid,int point)
        {
            string sql = string.Format("update action set point='{0}' where infoid={1}", point, infoid);

            try
            {
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {

                return "设置失败!";
            }

            return "设置完成!";
        }

        /// <summary>
        /// 批量更新扣费
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="point"></param>
        /// <returns></returns>
        public static string setPointAll(int infoid, int point)
        {
            string sql = string.Format("update action set point='{0}' where productid={1}", point, infoid);

            try
            {
               int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }


        /// <summary>
        /// 设置下游分成
        /// </summary>
        /// <param name="companyid"></param>
        /// <param name="point"></param>
        /// <returns></returns>
        public static string setCPdivideInto(int companyid,int productid,int point)
        {
            string sql = string.Format("update action set divideInto='{0}' where companyid={1} and productid={2}", point, companyid, productid);
            try
            {
               int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {

                return "设置失败!";
            }

            return "设置完成!";
        }


        /// <summary>
        /// 设置上游分成
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="point"></param>
        /// <returns></returns>
        public static string setPDdivideInto(int infoid, int point)
        {
            string sql = string.Format("update product set divideInto='{0}' where infoid={1}", point, infoid);
            try
            {
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                
            }
            catch (Exception e)
            {

                return "设置失败!";
            }

            return "设置完成!";
        }


        /// <summary>
        /// 重命名
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        public static string Rename(string idlist,int level,string newname)
        {
            string sql = string.Empty;

            if (level == 0)
                sql = string.Format("update conduit set names='{0}' where infoid in({1})", newname.Trim(), idlist);
            else if (level == 1)
                sql = string.Format("update product set name='{0}' where infoid in({1})", newname.Trim(), idlist);
            else if (level == 2)
                sql = string.Format("update company set name='{0}' where infoid in({1})", newname.Trim(), idlist);

            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string setActionInfo(string data,int infoid)
        {
            string sql=null;
            string param=null;
            try
            {
                if (!string.IsNullOrEmpty(data))
                {
                    param = getJsonSqlparams(data);
                    if (!string.IsNullOrEmpty(param))
                    {
                        sql = string.Format("update action set {0} where infoid={1}",param,infoid);
                        int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                    }
                }
            }
            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }


        /// <summary>
        /// 设置同步状态
        /// </summary>
        /// <param name="idlist"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string setSyncFlag(string idlist,int flag)
        {
            string sql = string.Format("update action set SyncFlag={0} where infoid in({1})", flag, idlist);
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }



        /// <summary>
        /// 设置同步方式
        /// </summary>
        /// <param name="idlist"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string setSyncMethod(string idlist, string data)
        {
            string sql = string.Format("update action set syncMethod='{0}' where infoid in({1})", data, idlist);
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }


        /// <summary>
        /// 设置下游业务状态
        /// </summary>
        /// <param name="idlist"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string setSyncStart(string idlist, int flag)
        {
            string sql = string.Format("update action set syncStart={0} where infoid in({1})", flag, idlist);
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }


        public static int setSyncStart(string idlist, int productid, int flag)
        {
            int old = 0;
            string sql = string.Format("update action set syncStart={0} where productid={1} and companyid in({2})", flag, productid, idlist);
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
            }
            catch (Exception e)
            {
                return -1;
            }

            return old;
        }


        /// <summary>
        /// 设置上游业务状态
        /// </summary>
        /// <param name="idlist"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static int setStartFlag(string idlist, int flag)
        {
            int old = 0;
            string sql1 = string.Format("update product set startFlag={0} where infoid in({1})", flag, idlist);
            string sql2 = string.Format("update action set syncStart={0} where productid in({1})", flag, idlist);

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    int id1 = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql1), 0);
                    int id2 = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql2), 0);
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    return -1;
                    // throw ex;
                }
                finally
                {
                    conn.Close();
                }
            }

            return old;
        }


        /// <summary>
        /// 添加产品规则
        /// </summary>
        /// <param name="productid"></param>
        /// <param name="companyid"></param>
        /// <param name="actionid"></param>
        /// <param name="typeid"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string setRuleInfo(int productid, int companyid, int actionid, int typeid, string info)
        {
            JObject json=new JObject();
            string datainfo = "";
            if (!string.IsNullOrEmpty(info))
            {
                if (typeid ==0)
                {
                    json = JObject.Parse(info);
                    int st = Convert.ToInt32(json["stime"].ToString().Replace(":", ""));
                    int et = Convert.ToInt32(json["etime"].ToString().Replace(":", ""));
                    if (st == 0 && et == 2359)
                    {
                        json.Remove("stime");
                        json.Remove("etime");
                    }
                    if (string.IsNullOrEmpty(json["disabled"].ToString().Trim()))
                        json.Remove("disabled");

                    datainfo = json.ToString().Replace(" ","");
                }
                else if (typeid == 1)
                {
                    JObject info1 = JObject.Parse("{\"word\":\"\",\"area\":\"\",\"result\":\"\",\"datatime\":\"\",\"flag\":\"\",\"message\":\"\",\"outtime\":10}");
                     
                    JArray list = JArray.Parse(info);

                    foreach(JObject item in list)
                    {
                        if (item["message"].ToString() == "99")
                        {
                            info1.Remove("area");
                            info1["flag"] = "d-99";
                            info1["message"] = "通道量满";
                        }
                        else
                        {
                            info1["flag"] = "d";
                            info1["message"] = "省份量满";
                        }
                        info1["word"] = item["word"].ToString();
                        datainfo += ","+info1.ToString().Replace(" ", "");
                    }
                    datainfo = "["+datainfo.Substring(1)+"]";
                }
                else if (typeid == 3 || typeid == 5)
                {
                    json = JObject.Parse(info);
                    if(Convert.ToInt32(json["day"])==0)
                        json.Remove("day");
                    if(Convert.ToInt32(json["month"])==0)
                        json.Remove("month");
                    datainfo = json.ToString();
                }
            }
            else
                return "内容为空，设置失败!";

            string sql = "insert into rules(productid,companyid,limit,typeid) values(" + productid + "," + companyid + ",'" + datainfo + "'," + typeid + ")";
            
            if (actionid>0)
                sql = "insert into rules(productid,actionid,limit,typeid) values(" + productid + "," + actionid + ",'" + datainfo + "'," + typeid + ")";

            try
            {
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }

        /// <summary>
        /// 更新规则
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string updateRuleInfo(int infoid, string info)
        {
            try
            { 
                string sql = string.Format("update rules set limit='{0}' where infoid={1}",info,infoid);
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }

        public static string updateRuleInfo(int infoid, int typeid, string info)
        {
            JObject json = JObject.Parse(info);       
            string sql = null;
            if (!string.IsNullOrEmpty(info))
            {
                if (typeid == 1)
                {
                    JObject info1 = JObject.Parse("{\"word\":\"\",\"area\":\"\",\"result\":\"\",\"datatime\":\"\",\"flag\":\"\",\"message\":\"\",\"outtime\":10}");

                    if (json["message"].ToString() == "999")
                    {
                        info1.Remove("area");
                        info1["flag"] = "999";
                        info1["message"] = "今日通道量满";
                    }
                    else if (json["message"].ToString() == "9999")
                    {
                        info1.Remove("area");
                        info1["flag"] = "9999";
                        info1["message"] = "本月通道量满";
                    }
                    else
                    {
                        info1["flag"] = "99";
                        info1["message"] = "省份量满";
                    }
                    info1["word"] = json["word"].ToString();
                    sql = string.Format("update rules set limit='['+replace(replace(limit,']',''),'[','')+',{0}]' where infoid={1}", info1.ToString(), infoid);
                }
                else if (typeid == 3)
                {
                    if (Convert.ToInt32(json["month"]) == 0)
                        json.Remove("month");

                    sql = string.Format("update rules set limit='{0}' where infoid={1}", json.ToString().Replace(" ", ""), infoid);
                }
                else if(typeid==5)
                    sql = string.Format("update rules set limit='{0}' where infoid={1}", json.ToString().Replace(" ", ""), infoid);
                else
                {
                    int st = Convert.ToInt32(json["stime"].ToString().Replace(":", ""));
                    int et = Convert.ToInt32(json["etime"].ToString().Replace(":", ""));
                    if (st == 0 && et == 2359)
                    {
                        json.Remove("stime");
                        json.Remove("etime");
                    }
                    if (string.IsNullOrEmpty(json["disabled"].ToString().Trim()))
                        json.Remove("disabled");

                    sql = string.Format("update rules set limit='{0}' where infoid={1}", json.ToString().Replace(" ", ""), infoid);
                }
                
            }
            else
                return "内容为空，设置失败!";

            try
            {
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }

            return "设置完成!";
        }


        /// <summary>
        /// 删除规则
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string delRuleInfo(string infoid)
        {
            string sql = string.Format("delete rules where infoid in({0})", infoid);
            try
            {
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }


        /// <summary>
        /// 设置规则内容
        /// </summary>
        /// <param name="info"></param>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string setRuleLimitItem(string info, int infoid)
        {
            
            try
            {
                string sql = string.Format("update rules set limit='{0}' where infoid={1}", info, infoid);
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }


        /// <summary>
        /// 设置cp相关信息
        /// </summary>
        /// <param name="field"></param>
        /// <param name="data"></param>
        /// <param name="companyid"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string setCPPropertyInfo(string field, string data, int companyid, int flag)
        {
            try
            {
                JArray json = JArray.Parse(data);
                string sql = string.Format("update cppropertyInfo set {0}='{1}' where companyid={2} and flag={3}", field, Utils.ClearBR(json.ToString()), companyid, flag);
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }



        /// <summary>
        /// 添加账单相关信息
        /// </summary>
        /// <param name="field"></param>
        /// <param name="data"></param>
        /// <param name="companyid"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string addCPPropertyInfo(string field, string data, int companyid, int flag)
        {
            try
            {
                /*string sql ="insert into Invoice('contact','account','invoice','company','companyID','flag')";

                JArray json = JArray.Parse(data);
                foreach (JObject jsonObj in json)
                {
                    IEnumerable<JProperty> properties = jsonObj.Properties();
                    foreach (JProperty item in properties)
                    {
                        names += "," + item.Name.ToString();
                        value += "," + item.Value.ToString();
                    }
                }*/

                JArray json = JArray.Parse(data);
                string sql = string.Format("insert into cppropertyInfo({0},companyid,flag) values('{1}',{2},{3})", field, Utils.ClearBR(json.ToString()), companyid, flag);

                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "添加失败!";
            }
            return "添加完成!";
        }


        /// <summary>
        /// 设置后台登陆标识
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string setLoginFlag(int infoid,int value)
        {
            try
            {
                string sql = string.Format("update company set login={0} where infoid={1}", value, infoid);

                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }

            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }


        /// <summary>
        /// 设置结算单状态
        /// </summary>
        /// <param name="data"></param>
        /// <param name="payflag"></param>
        /// <param name="cpflag"></param>
        /// <returns></returns>
        public static bool setBillState(string data, int payflag, int cpflag)
        {
            bool flag = true;
            JArray json = JArray.Parse(data);

            if (json.Count > 0)
            {
                SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
                conn.Open();
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    try
                    {
                        string infoid = "";
                        if (payflag != 1) /*非支付操作*/
                        {
                            /*将public_statistics 统计表的数据支付状态设置为未支付*/
                            for (int i = 0; i < json.Count; i++)
                            {
                                infoid += "," + json[i]["infoid"].ToString();
                                JArray list = JArray.Parse(json[i]["info"].ToString());
                                for (int j = 0; j < list.Count; j++)
                                    SqlHelper.ExecuteNonQuery(trans, CommandType.Text, "update public_statistics set " + (cpflag == 1 ? "pflag=0" : "rflag=0") + " where companyid=" + list[j]["companyID"].ToString() + " and productid=" + list[j]["productID"].ToString());
                            }
                            if (payflag == 2) /*已支付--作废操作*/
                                SqlHelper.ExecuteNonQuery(trans, CommandType.Text, "update settlementInfo set payflag=2 where infoid in(" + infoid.Substring(1) + ")");
                            else /*未支付--作废操作*/
                            {
                                SqlHelper.ExecuteNonQuery(trans, CommandType.Text, "delete settlementInfo where infoid in(" + infoid.Substring(1) + ")");
                                SqlHelper.ExecuteNonQuery(trans, CommandType.Text, "delete settlementSheet where id in(" + infoid.Substring(1) + ")");
                            }
                        }
                        else /*支付操作*/
                            SqlHelper.ExecuteNonQuery(trans, CommandType.Text, "update settlementInfo set payflag=1 where infoid in(" + infoid.Substring(1) + ")");


                        trans.Commit();
                    }
                    catch (Exception e)
                    {
                        flag = false;
                        trans.Rollback();
                        LogHelper.WriteLog(typeof(dataManage), "============>setBillState异常[" + e.ToString() + "]");
                    }
                }
                conn.Close();
            }

            return flag;
        }

        /// <summary>
        /// 设置附件信息
        /// </summary>
        /// <param name="data"></param>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string setAttachInfo(string data,int infoid)
        {
            try
            {
                string sql = string.Format("update settlementInfo set attach=(case when len(attach)>0 then replace(attach,']','')+',{0}]' else '[{0}]' end) where infoid={2}", data, infoid);

                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }

            catch (Exception e)
            {
                return "设置失败!";
            }
            return "设置完成!";
        }

        /// <summary>
        /// 设置备注信息
        /// </summary>
        /// <param name="data"></param>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string setBillRemark(string data, int infoid)
        {
            try
            {
                string sql = string.Format("update settlementInfo set remarks="+(data=="NULL"?"{0}":"'{0}'")+" where infoid={1}", data, infoid);
                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
            }
            catch (Exception e)
            {
                return "{\"success\":false}";
            }
            return "{\"success\":true,data:{\"remarks\":\"" + (data == "NULL" ? "" : data) + "\"}}";
        }


        /// <summary>
        /// 获取结算方式
        /// </summary>
        /// <param name="status"></param>
        /// <param name="infoid"></param>
        /// <param name="level"></param>
        /// <returns></returns>
        public static int setSettlementType(int status, int infoid, int level)
        {
            string sql = string.Format("update " + (level == 1 ? "product" : "action") + " set settlement={0} where infoid={1}", status,infoid);
            int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql),0);

            return old;
        }


        /// <summary>
        /// 设置同步监控
        /// </summary>
        /// <param name="idlist"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string setAlarm(string idlist, int flag)
        {
            string sql = string.Format("update product set alarm='{0}' where infoid in({1})", flag, idlist);
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    int old = Utils.StrToInt(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql), 0);
                }
            }
            catch (Exception e)
            {
                return "{\"success\":false}";
            }

            return "{\"success\":true}";
        }

    }
}
