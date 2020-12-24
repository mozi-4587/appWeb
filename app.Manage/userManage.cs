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
   public class userManage
    {


       /// <summary>
       /// 用户登录
       /// </summary>
       /// <param name="user"></param>
       /// <returns></returns>
       public static string Login(string user)
       {
           string info =string.Empty;
           try
           {
               string sql = "select infoid,code from company where ename='" + user + "'";

               DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

               if (dt.Rows.Count > 0)
                   info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[","").Replace("]","");
           }
           catch (Exception e)
           {
               return null;
           }

           return info;
       }



       public static string saleLogin(string user)
       {
           string info = string.Empty;
           try
           {
               string sql = "select salename,code from saleInfo where salename='" + user + "'";

               DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

               if (dt.Rows.Count > 0)
                   info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
           }
           catch (Exception e)
           {
               return null;
           }

           return info;
       }

       public static string getSaleInfo(int pagesize, int currentpage, string param)
       {
           string info = "{\"data\":[]}";
           string paramstr = "";
           string sql = string.Empty;
           try
           {

               if (!string.IsNullOrEmpty(param))
               {
                   JObject datajson = JObject.Parse(param);
                   IEnumerable<JProperty> properties = datajson.Properties();
                   foreach (JProperty item in properties)
                   {
                       string names = item.Name.ToString();
                       paramstr += string.Format("and {0}='{1}'", names, item.Value.ToString());
                   }
               }

               int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(infoid) FROM saleInfo WHERE infoid>0 " + paramstr));

               if (currentpage == 1)
                   sql = "SELECT TOP " + pagesize + " * FROM saleInfo WHERE " + paramstr + " order by infoid desc";
               else
                   sql = "SELECT TOP " + pagesize + " * FROM saleInfo WHERE infoid <=(SELECT MIN(infoid) FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM saleInfo WHERE infoid>0 " + paramstr + " ORDER BY [infoid] DESC) AS [tblTmp]) and ORDER BY infoid DESC";
                      

               DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

               if (dt.Rows.Count > 0)
                   info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";
               
           }
           catch (Exception e)
           {
               return null;
           }

           return info;
       }


       public static string getSaleInfo(int uid)
       {
           string info = string.Empty;
           try
           {
               string sql = "select * from saleInfo where infoid=" + uid;

               DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

               if (dt.Rows.Count > 0)
               {
                   info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                   info = "{\"success\":true,\"data\":" + info + "}";
               }
           }
           catch (Exception e)
           {
               return null;
           }

           return info;
       }

       public static string getSaleInfo(string uid)
       {
           string info = string.Empty;
           try
           {
               string sql = "select * from saleInfo where salename='" + uid+"'";

               DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

               if (dt.Rows.Count > 0)
               {
                   info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                   info = "{\"success\":true,\"data\":" + info + "}";
               }
           }
           catch (Exception e)
           {
               return null;
           }

           return info;
       }


       public static int setSaleInfo(string data,string uid)
       {
           int flag = -1;
          
           string field = string.Empty;
           if (!string.IsNullOrEmpty(data))
           {
               JObject datajson = JObject.Parse(data);
               IEnumerable<JProperty> properties = datajson.Properties();
               foreach (JProperty item in properties)
                   field += "," + item.Name.ToString()+"='"+item.Value.ToString()+"'";

               string sql = "update saleInfo set " + field.Substring(1) + " where salename='" + uid + "'";
               flag=Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql));
           }

           return flag;
       }



       public static int addSaleInfo(string data)
       {
           int flag = 0;
           string value = string.Empty;
           string field = string.Empty;
           if (!string.IsNullOrEmpty(data))
           {
               JObject datajson = JObject.Parse(data);
               IEnumerable<JProperty> properties = datajson.Properties();
               foreach (JProperty item in properties)
               {
                   field += "," + item.Name.ToString();
                   value += ",'" + item.Value.ToString() + "'";
               }
               string sql = "insert into saleInfo(" + field.Substring(1) + ") values(" + value.Substring(1) + ")";
               flag=Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql));
           }

           return flag;
       }


       public static string getUserInfo(int uid)
       {
           string info = string.Empty;
           try
           {
               string sql = "select name,ename from company where infoid=" + uid;

               DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

               if (dt.Rows.Count > 0)
               {
                   info =JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                   info = "{\"success\":true,\"data\":" + info + "}";
               }
           }
           catch (Exception e)
           {
               return null;
           }

           return info;
       }





       /// <summary>
       /// 获取用户产品数据
       /// </summary>
       /// <param name="infoid"></param>
       /// <returns></returns>
       public static string getProductInfo(int infoid)
       {
           string info = "{\"data\":[]}";
           string paramstr = "";
           if (infoid == 0)
               return info;

           string cp = getCompanyInfo(infoid);

           if (string.IsNullOrEmpty(cp))
               return info;
           else
           {
               JObject cpinfo = JObject.Parse(cp);
               if (!string.IsNullOrEmpty(cpinfo["business"].ToString()))
                   paramstr= " and a.conduitid in(" + cpinfo["business"].ToString() + ")";
               else
                   return info;
           }

           string sql = "SELECT a.productID as infoid,(select name from product where infoid=a.productID) as names FROM action as a where  a.companyID=" + infoid + paramstr+" group by a.productid order by names";
           DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

           if (dt.Rows.Count > 0)
           {
               DataRow dr = dt.NewRow();
               dr["infoid"] = -1;
               dr["names"] = "全部业务";
               dt.Rows.Add(dr);
               dt.AcceptChanges();
               info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

           }

           return info;
       }

      


       /// <summary>
       /// 获取同步数据
       /// </summary>
       /// <returns></returns>
       public static string getDataInfo(int pagesize, int currentpage, int companyid, string param)
       {
           string info = "{data:[],total:0}";
          
           string paramstr = "";
           string sql = string.Empty;

           DateTime smonth = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(-2).AddDays(0);
           DateTime emonth = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);

           if (companyid == 0)
               return info;
           else
               paramstr = "a.companyid=" + companyid;

           string cp = getCompanyInfo(companyid);

           if (string.IsNullOrEmpty(cp))
               return info;
           else
           {
               JObject cpinfo = JObject.Parse(cp);
               if (!string.IsNullOrEmpty(cpinfo["business"].ToString()))
                   paramstr += " and a.conduitid in(" + cpinfo["business"].ToString() + ")";
               else
                   return info;
           }


           if (!string.IsNullOrEmpty(param))
           {
               JObject datajson = JObject.Parse(param);

               if (!string.IsNullOrEmpty(datajson["info"].ToString()))
               {
                   if(datajson["typeid"].ToString()=="0")
                       paramstr += " and (a.userOrder like '" + datajson["info"].ToString() + "%' or a.StreamingNo like '" + datajson["info"].ToString() + "')";
                   else
                       paramstr += " and a.mobile like '" + datajson["info"].ToString() + "%'";
               }

               if (!string.IsNullOrEmpty(datajson["product"].ToString()))
               {
                   if (Convert.ToInt32(datajson["product"]) > 0)
                       paramstr += " and a.productID=" + datajson["product"].ToString();
  
               }


               if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
               {
                   DateTime newdate = Convert.ToDateTime(datajson["sdate"].ToString());
                   if (newdate.CompareTo(smonth) > 0)
                       paramstr += " and a.datatime between '" + datajson["sdate"].ToString() + "' and  DATEADD(dd,+1,'" + datajson["edate"].ToString() + "')";
                   else
                       paramstr += " and a.datatime between '" + smonth.ToString("yyyy-MM-dd") + "' and  DATEADD(dd,+1,'" + datajson["edate"].ToString() + "')";
               }
               else

                   paramstr += " and a.datatime between '" + smonth.ToString("yyyy-MM-dd") + "' and  '" + emonth.ToString("yyyy-MM-dd")+"'";
               
               

           }
           int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM public_sync_2017 as a WITH(NOLOCK) WHERE " + paramstr));

           if (currentpage == 1)
               sql = "SELECT TOP " + pagesize + " c.name as company,(case when len(a.userOrder)>0 then a.userOrder else a.StreamingNo end) as userOrder,(select name from product where infoid=a.productid) as product,a.IMSI,a.mobile,a.area,(case when conduitid=90 then Convert(decimal(18,2),a.fee/100) else Convert(decimal(18,2),a.fee) end) as price,a.status,a.optype,a.datatime"
                   + " FROM public_sync_2017 as a WITH(NOLOCK)"
                   + " left join company as c on c.infoid=a.companyid"
                   + " where "+paramstr+" order by a.datatime desc";
                 
           else
           {
               sql = "SELECT TOP " + pagesize + " c.name as company,(select name from product where infoid=a.productid) as product,(case when len(a.userOrder)>0 then a.userOrder else a.StreamingNo end) as userOrder,a.IMSI,a.mobile,a.area,(case when conduitid=90 then Convert(decimal(18,2),a.fee/100) else Convert(decimal(18,2),a.fee) end) as price,a.status,a.optype,a.datatime,a.resultCode"
                   + " FROM public_sync_2017 as a WITH(NOLOCK)"
                   + " left join company as c on c.infoid=a.companyid"
                   + " WHERE a.infoid <=(SELECT MIN(infoid)"
                   + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM public_sync_2017 WITH(NOLOCK)"
                   + " WHERE " + paramstr.Replace("a.", "") + " ORDER BY [datatime] DESC) AS [tblTmp]) and "
                   + paramstr + " ORDER BY a.datatime DESC";
           }

           DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

           if (dt.Rows.Count > 0)
               info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";

           return info;

       }


       /// <summary>
       /// 获取同步配置表
       /// </summary>
       /// <param name="conduitid"></param>
       /// <returns></returns>
       static string getConfigTable(int conduitid)
       {
           string info = string.Empty;
           string sql = "select tablename from public_Config where conduitid=" + conduitid + " and step=4";

           object obj = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
           if (obj != System.DBNull.Value)
              info = obj.ToString();

           return info;
           
       }


       static int getConduitid(int productid)
       {
           string sql = "select conduitid from product where infoid=" + productid;

           return Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql));
       }

       /// <summary>
       /// 密码修改
       /// </summary>
       /// <param name="uid"></param>
       /// <param name="code"></param>
       /// <returns></returns>
       public static int setUserCode(int uid, string code)
       {
           int old = 0;
           string sql = "update company set code='" + code + "' where infoid=" + uid;

           old = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

           return old;
       }


       /// <summary>
       /// 收入统计
       /// </summary>
       /// <param name="uid"></param>
       /// <param name="param"></param>
       /// <returns></returns>
       public static string getIncomeStatistics(int uid, string param)
       {
           string info = "{\"totalAmount\":0,\"unsubscribe\":0}";
           
           DateTime smonth = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(-2).AddDays(0);
           DateTime emonth = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);
           

           string paramstr = string.Empty;
          
           if (uid == 0)
               return info;
           else
               paramstr = "a.companyid=" + uid;

           string cp = getCompanyInfo(uid);

           if (string.IsNullOrEmpty(cp))
               return info;
           else
           {
               JObject cpinfo = JObject.Parse(cp);
               if (!string.IsNullOrEmpty(cpinfo["business"].ToString()))
                   paramstr += " and a.conduitid in(" + cpinfo["business"].ToString() + ")";
               else
                   return info;
           }

           if (!string.IsNullOrEmpty(param))
           {
               JObject datajson = JObject.Parse(param);

               if (!string.IsNullOrEmpty(datajson["product"].ToString()))
               {
                   if (Convert.ToInt32(datajson["product"]) > 0)
                       paramstr += " and a.productID=" + datajson["product"].ToString();
               }

               if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
               {
                   DateTime newdate=Convert.ToDateTime(datajson["sdate"].ToString());
                   if (newdate.CompareTo(smonth) > 0)
                      paramstr += " and a.datatime between '" + datajson["sdate"].ToString() + "' and  DATEADD(dd,+1,'" + datajson["edate"].ToString() + "')";
                   else
                      paramstr += " and a.datatime between '" + smonth.ToString("yyyy-MM-dd") + "' and  DATEADD(dd,+1,'" + datajson["edate"].ToString() + "')";
               }
               else
               {
                    DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);
                    paramstr += " and a.datatime between '" + DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01' and '" + monthDays.ToString("yyyy-MM-dd") + "'";
               }
           }
           else
           {
               DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);
               paramstr += " and datatime between '" + DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01' and '" + monthDays.ToString("yyyy-MM-dd") + "'";
           }
           string sql = "select(case when totalAmount is null then 0 else totalAmount end) as totalAmount,(case when unsubscribe is null then 0 else unsubscribe end) as unsubscribe from(select sum(case when a.optype=0 then Convert(decimal(18,2),case when b.minUnit=1 then a.fee/100 else a.fee end) else 0 end) as totalAmount,"
                       + "sum(case when a.optype=1 then Convert(decimal(18,2),case when b.minUnit=1 then a.fee/100 else a.fee end) else 0 end) unsubscribe "
                       //+"sum(case when b.amount>0 then b.amount else 0 end) as amount "

                       + "from public_sync_2017 as a WITH(NOLOCK) left join product as b on b.infoid=a.productID "
                       + "where " + paramstr + " and a.resultCode=0 and a.result=0) as tab";

           DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

           if (dt.Rows.Count > 0)
               info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");

           return info;

       }


       /// <summary>
       /// 业务项目金额统计
       /// </summary>
       /// <param name="uid"></param>
       /// <param name="param"></param>
       /// <returns></returns>
       public static string getHistogramStatistics(int uid, string param)
       {
           string info = string.Empty;
           string idlist = string.Empty;
           string paramstr = string.Empty;
           string terms= string.Empty;

           DateTime smonth = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(-2).AddDays(0);
           DateTime emonth = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);

           if (uid == 0)
               return info;
           else
               paramstr = "a.companyid=" + uid;

           string cp = getCompanyInfo(uid);

           if (string.IsNullOrEmpty(cp))
               return info;
           else
           {
               JObject cpinfo = JObject.Parse(cp);
               if (!string.IsNullOrEmpty(cpinfo["business"].ToString()))
                   paramstr += " and a.conduitid in("+cpinfo["business"].ToString()+")";
               else
                   return info;
           }

           if (!string.IsNullOrEmpty(param))
           {
               JObject datajson = JObject.Parse(param);

               if (!string.IsNullOrEmpty(datajson["product"].ToString()))
               {
                   if (Convert.ToInt32(datajson["product"]) > 0)
                       paramstr += " and a.productID=" + datajson["product"].ToString();

               }

               if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
               { 
                   DateTime newdate = Convert.ToDateTime(datajson["sdate"].ToString());
                   if (newdate.CompareTo(smonth) > 0)
                       terms += " and datatime between '" + datajson["sdate"].ToString() + "' and  DATEADD(dd,+1,'" + datajson["edate"].ToString() + "')";
                   else
                       terms += " and datatime between '" + smonth.ToString("yyyy-MM-dd") + "' and  DATEADD(dd,+1,'" + datajson["edate"].ToString() + "')";
               }
               else
               {
                   DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);
                   terms = " and datatime between '" + DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01' and '" + monthDays.ToString("yyyy-MM-dd") + "'";
               }
           }
           else
           {
               DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(0);
               terms = " and datatime between '" + DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01' and '" + monthDays.ToString("yyyy-MM-dd") + "'";
           }
           string sql = "SELECT (select name from product where infoid=a.productID) as names,(select case when sum(fee)>0 then sum(fee) else 0 end from public_sync_2017 where productID=a.productID and companyID=" + uid + " and resultCode=0 and optype=0" + terms + ") as price FROM action as a where " + paramstr + " group by a.productID order by names";
          // string sql= "SELECT (select names from conduit where infoid=a.conduitID) as names,(select sum(fee) from public_sync_2017 where " + paramstr.Replace("a.", "") + " and resultCode=0 and optype=0 and result=0) as price FROM action as a where " + paramstr + " and conduitid>0 group by a.conduitID order by names";
         
           DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

           if (dt.Rows.Count > 0)
               info = JsonConvert.SerializeObject(dt, new DataTableConverter());

           return info;
       }


       static string getCompanyInfo(int uid)
       {

           string info =null;
           string sql = "select top 1 * from company  where infoid=" + uid;


           DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

           if (dt.Rows.Count > 0)
               info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[","").Replace("]","");


           return info;
       }





    }
}
