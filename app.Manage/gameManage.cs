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
    public class gameManage
    {

        /// <summary>
        /// 获取下游公司列表
        /// </summary>
        /// <returns></returns>
        public static string getRulesList()
        {
            string info = "{data:[]}";
            string sql = "select infoid from rules order by infoid";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {

                DataTable dcopy = dt.Copy();
                DataView dv = dt.DefaultView;
                dv.Sort = "infoid asc";
                dcopy = dv.ToTable();

                info = "{data:" + JsonConvert.SerializeObject(dcopy, new DataTableConverter()) + "}";
            }

            return info;
        }
        /// <summary>
        /// 插入屏蔽手机号数据
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string saveBlacklist(string mobilelist, string ruleslist)
        {
            string retvalue = "";
            int olid = 0;


            try
            {

                string[] strarr = mobilelist.Replace("\n","").Split((char)'/');
                for (int i = 0; i < strarr.Length; i++)
                {
                    if (IsHandseta(strarr[i]))
                    {
                        int flag = Verifymbile(strarr[i]);
                        if (flag > 0)
                        {
                            retvalue += strarr[i] + "(已存在)/";
                            continue;

                        }
                        string sql = "insert into blacklist(mobile,rulesID) values('" + strarr[i] + "',3)";
                        olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                        if (olid < 1)
                            retvalue += strarr[i] + "(未添加成功)/";
                    }
                    else
                    {
                        retvalue += strarr[i] + "(号码不正确)/";
                    }
                }
            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>black数据插入异常[" + e.ToString() + "]");
                return retvalue;
            }
            return retvalue;
        }


        public static int Verifymbile(string info)
        {
            int flag = 0;
            string sql = "select CASE WHEN (select count(infoid) as num from [blacklist]  where mobile='" + info + "')>0  then 1  ELSE 0  END";
            object old = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (DBNull.Value != old)
                flag = Convert.ToInt32(old);

            return flag;
        }




        public static bool IsHandseta(string str_handset)   //^[1]+[3,5]+\d{9}
        {
            return System.Text.RegularExpressions.Regex.IsMatch(str_handset, @"^[-]?[1-9]{1}\d*$|^[0]{1}$");
        }




        /// <summary>
        /// 获取公司列表
        /// </summary>
        /// <returns></returns>
        public static string getCompanyList(int pagesize, int currentpage, string conduitid, string manatreeDepthid)
        {
            string info = "{data:[]}";
            // string sql = "select infoid,name,code,syncUrl from company order by infoid";
            string sql = null;
            string wherebf = " where ";
            if (conduitid == "99999")
            {
              



                int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "select count(*) from company   "));


                if (currentpage == 1)
                    sql = "SELECT TOP " + pagesize + "  infoid,name,code,syncUrl from company  order by infoid desc";
                else
                {
                    sql = "SELECT TOP " + pagesize + " infoid,name,code,syncUrl from company  WHERE   [infoid] <(SELECT MIN(infoid) ";
                    sql += "FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM company order by infoid desc) AS [tblTmp]) order by infoid desc";
                }

                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt.Rows.Count > 0)

                    info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",totalCount:" + TotalRecords + "}";
            }
            else
            {


                if (manatreeDepthid == "5")
                {
                    wherebf += " productid='" + conduitid + "'";
                }
                else
                {
                    if (conduitid == "0")
                    {
                        wherebf += " 1=1 ";
                    }
                    else
                    {
                        wherebf += " conduitid='" + conduitid + "'";
                    }
                }



                int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "select count(*) from company where infoid in(select companyid from conRelcompany " + wherebf + ") "));


                if (currentpage == 1)
                    sql = "SELECT TOP " + pagesize + "  infoid,name,code,syncUrl from company where infoid in(select companyid from conRelcompany " + wherebf + ") order by infoid desc";
                else
                {
                    sql = "SELECT TOP " + pagesize + " infoid,name,code,syncUrl from company  WHERE  infoid in(select companyid from conRelcompany " + wherebf + ") and [infoid] <(SELECT MIN(infoid) ";
                    sql += "FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM company order by infoid desc) AS [tblTmp]) order by infoid desc";
                }

                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt.Rows.Count > 0)

                    info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",totalCount:" + TotalRecords + "}";
            }

            return info;


        }
        /// <summary>
        /// 获取指令列表
        /// </summary>
        /// <returns></returns>
        public static string getActionList(string infoid)
        {
            string info = "{data:[]}";
            string sql = "select infoid, ordered, unsubscribe,price,buyname,syncflag,syncurl,syncstart,syncmethod,optypeid,point from action where companyid='" + infoid + "' order by infoid";


            // string sql = null;


            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "select count(*) from action  where companyid='" + infoid + "'"));




            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",totalCount:" + TotalRecords + "}";

            return info;


        }
        /// <summary>
        /// 获取一条指令
        /// </summary>
        /// <returns></returns>
        public static string getActionRecord(string infoid)
        {
            string info = "{success:true,data:{}}";
            string sql = "select syncurl,point from action where infoid='" + infoid + "' order by infoid";


            // string sql = null;


          //  int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "select count(*) from action  where companyid='" + infoid + "'"));




            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {

                info = "{success:true,data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }

            return info.Replace("[","").Replace("]","");


        }
        /// <summary>
        /// 插入公司数据
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string addCompany(string name, string code, string syncUrl)
        {
            string retvalue = "";
            int olid = 0;


            try
            {


                string sql = "INSERT INTO [company](name,code,syncUrl) VALUES('" + name + "','" + code + "','" + syncUrl + "')";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(gameManage), "============>black数据插入失败[" + name + "]");


            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>black数据插入异常[" + e.ToString() + "]");
                retvalue = e.ToString();
                return retvalue;
            }
            return retvalue;
        }
        /// <summary>
        /// 获取指令列表
        /// </summary>
        /// <returns></returns>
        public static string getProductList()
        {
            string info = "data:[]";
            string sql = "select infoid as productid, name as productname from product  order by infoid";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;


        }
        /// <summary>
        /// 增加指令表
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string addAction(string operators, string companyid, string productcode, string ordered, string unsubscribe, string omessage, string umessage, string price, string conduitid, string productid, string buyid, string buyname, string rulesid, string syncflag, string syncurl, string syncstart, string syncmethod, string optypeid, string point)
        {
            string retvalue = "";
            int olid = 0;


            try
            {


                string sql = "INSERT INTO action(operators,companyid,productcode,ordered, unsubscribe,omessage,umessage,price,conduitid,productid,buyid,buyname,rulesid,syncflag,syncurl,syncstart,syncmethod,optypeid,point)"
                + " VALUES('" + operators + "','" + companyid + "','" + productcode + "','" + ordered + "','" + unsubscribe + "','" + omessage + "','" + umessage + "',"
                + "'" + price + "','" + conduitid + "','" + productid + "','" + buyid + "','" + buyname + "','" + rulesid + "','" + syncflag + "',"
                + " '" + syncurl + "','" + syncstart + "','" + syncmethod + "','" + optypeid + "','" + point + "')";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                string sqlls = "insert into conrelcompany(companyid,conduitid,productid)values('" + companyid + "','" + conduitid + "','" + productid + "')";
                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqlls);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(gameManage), "============>black数据插入失败[" + companyid + "]");


            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>black数据插入异常[" + e.ToString() + "]");
                retvalue = e.ToString();
                return retvalue;
            }
            return retvalue;
        }
        /// <summary>
        /// 验证用户登录
        /// </summary>
        /// <returns></returns>
        public static bool checkUser(string username,string password)
        {
            bool retvalue = false;
            string sql = "select count(*) from userinfo where username='"+username.Trim()+"' and password='"+password.Trim()+"'";
            object str = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

            if (int.Parse(str.ToString()) > 0)
            {

                retvalue = true;
            }

            return retvalue;
        }

             /// <summary>
        /// 修改改指令状态
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string updateActionstate(string actionListid,string  comboType,string comboState)
        {
            string retvalue = "";
            int olid = 0;


            try
            {


                string sql = "update action set "+comboType+"='"+comboState+"' where infoid in("+actionListid+")";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(gameManage), "============>action修改数据失败[" + actionListid + "]");


            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>action修改数据失败[" + e.ToString() + "]");
                retvalue = e.ToString();
                return retvalue;
            }
            return retvalue;
        }
        /// <summary>
        /// 修改改指令状态
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string updateAction(string actionid, string syncurl, string point)
        {
            string retvalue = "";
            int olid = 0;


            try
            {


                string sql = "update action set syncurl='" + syncurl + "',point='" + point + "' where infoid ='" + actionid + "'";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(gameManage), "============>action修改数据失败[" + actionid + "]");


            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>action修改数据失败[" + e.ToString() + "]");
                retvalue = e.ToString();
                return retvalue;
            }
            return retvalue;
        }

        /// <summary>
        /// 获取通道目录数据
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string getConduitTree(int id, string depthdepthlength)
        {

            string sql = "";// "select a.infoid,a.names,(select COUNT(infoid) from conduit as b where a.infoid=b.pid) as childnode from conduit as a where a.pid='" + id + "' order by a.infoid";
            if (depthdepthlength == "4")
            {
                sql = "select a.infoid,a.name as names ,(select COUNT(infoid) from product as b where a.infoid=b.pid) as childnode from product as a where a.conduitid='" + id + "' order by a.infoid";
            }
            else
            {
                if (depthdepthlength == "3")
                {
                    sql = "select a.infoid,a.names,(select COUNT(infoid) from product as b where a.infoid=b.conduitid) as childnode from conduit as a where a.pid='" + id + "' order by a.infoid";
                }
                else
                {

                    sql = "select a.infoid,a.names,(select COUNT(infoid) from conduit as b where a.infoid=b.pid) as childnode from conduit as a where a.pid='" + id + "' order by a.infoid";
                }
            }
            //string sql = "select a.infoid,a.names from conduit as a where a.pid='" + id + "' order by a.infoid";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            IList<treemanageModel> model = new List<treemanageModel>();
            foreach (DataRow row in dt.Rows)
            {
                treemanageModel item = new treemanageModel();
                item.infoidm = Convert.ToInt32(row["infoid"]);
                item.text = row["names"].ToString();
               
                item.childnode = Utils.StrToInt(row["childnode"], 0);
               
                model.Add(item);
            }
            return JsonConvert.SerializeObject(model);

        }
        /// <summary>
        /// 增加通道表
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string addConduit(string pid, string names,string price, string area,string down,string setfull,string fullnotify  )
        {
            string retvalue = "";
            int olid = 0;


            try
            {
                string sql = "INSERT INTO conduit(pid, names,price, area, down, setfull, fullnotify)"
                + " VALUES('" + pid + "','" + names + "','" + price + "','" + area + "','" + down + "','" + setfull + "','" + fullnotify + "')";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(gameManage), "============>black数据插入失败[" + names + "]");


            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>black数据插入异常[" + e.ToString() + "]");
                retvalue = e.ToString();
                return retvalue;
            }
            return retvalue;
        }
        /// <summary>
        /// 插入产品信息
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static string addProduct(string name, string address, string pid)
        {
            string retvalue = "";
            int olid = 0;

            try
            {
                string sql = "INSERT INTO [product](name,address,conduitid) VALUES('" + name + "','" + address + "','" + pid + "')";

                olid = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
                if (olid < 1)
                    LogHelper.WriteLog(typeof(gameManage), "============>black数据插入失败[" + name + "]");


            }
            catch (Exception e)
            {
                LogHelper.WriteLog(typeof(gameManage), "============>black数据插入异常[" + e.ToString() + "]");
                retvalue = e.ToString();
                return retvalue;
            }
            return retvalue;
        }
        /// <summary>
        /// 获取业务列表
        /// </summary>
        /// <returns></returns>
        public static string getConduitList()
        {
            string info = "data:[]";
            string sql = "select infoid as conduitid, names as conduitname from conduit  order by infoid";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;


        }
        /// <summary>
        /// 获取业务列表
        /// </summary>
        /// <returns></returns>
        public static string getConduitrecord(string  recordid)
        {
            string info = "{success:true,data:{}}";
         

            string sql = "select  names,area,down,setfull,fullnotify from conduit where infoid='" + recordid + "' order by infoid";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {

                info = "{success:true,data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }

            return info.Replace("[", "").Replace("]", "");


        }

    }
}
