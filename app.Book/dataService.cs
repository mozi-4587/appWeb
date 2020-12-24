using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

using app.Common;
using app.Entity;
using app.Data;

namespace app.Book
{
    public class dataService
    {


        /// <summary>
        /// 获取
        /// </summary>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string getBookInfo(int flag)
        {
            string info = null;
          
            string sql = "select top 4 * from book where flag="+flag;
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
               info = JsonConvert.SerializeObject(dt);
            

            return info;
        }

    }
}
