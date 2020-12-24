using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace app.Common
{
    public class GUID
    {

         /// <summary>
         /// 生成特定位数的唯一字符串
         /// </summary>
         /// <param name="num">特定位数</param>
         /// <returns></returns>
         public static string GenerateUniqueText(int num)
         {
             string randomResult = string.Empty;
             string readyStr = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
             char[] rtn = new char[num];
             Guid gid = Guid.NewGuid();
             var ba = gid.ToByteArray();
             for (var i = 0; i < num; i++)
             {
                 rtn[i] = readyStr[((ba[i] + ba[num + i]) % 35)];
             }
            foreach (char r in rtn)
             {
                 randomResult += r;
             }
            return randomResult;
         }

         /// <summary>  
         /// 生成22位唯一的数字 并发可用  
         /// </summary>  
         /// <returns></returns>  
         public static string GenerateUniqueID()
         {
             System.Threading.Thread.Sleep(1); //保证yyyyMMddHHmmssffff唯一  
             Random d = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
             string strUnique = DateTime.Now.ToString("yyyyMMddHHmmssffff") + d.Next(1000, 9999);
             return strUnique;
         }

         /// <summary>
         /// 生成GUID
         /// </summary>
         /// <param name="format">格式符</param>
         /// <returns></returns>
         public static string CreatGUID(string format)
         {
             var uuid = Guid.NewGuid().ToString(); // 9af7f46a-ea52-4aa3-b8c3-9fd484c2af12  
             var flat = format.ToUpper();

             if (flat == "N")
                 return Guid.NewGuid().ToString("N"); // e0a953c3ee6040eaa9fae2b667060e09   
             else if (flat == "B")
                 return Guid.NewGuid().ToString("B"); // {734fd453-a4f8-4c5d-9c98-3fe2d7079760}  
             else if (flat == "P")
                 return Guid.NewGuid().ToString("P"); //  (ade24d16-db0f-40af-8794-1e08e2040df3)  
             else if (flat == "X")
                 return Guid.NewGuid().ToString("X"); //{0x3fa412e3,0x8356,0x428f,{0xaa,0x34,0xb7,0x40,0xda,0xaf,0x45,0x6f}}
             else
                 return Guid.NewGuid().ToString("D"); // 9af7f46a-ea52-4aa3-b8c3-9fd484c2af12
         }

         /// <summary>  
         /// 根据GUID获取16位的唯一字符串  
         /// </summary>  
         /// <param name=\"guid\"></param>  
         /// <returns></returns>  
         public static string GuidTo16String()
         {
             long i = 1;
             foreach (byte b in Guid.NewGuid().ToByteArray())
                 i *= ((int)b + 1);
             return string.Format("{0:x}", i - DateTime.Now.Ticks);
         }
         /// <summary>  
         /// 根据GUID获取19位的唯一数字序列  
         /// </summary>  
         /// <returns></returns>  
         public static long GuidToLongID()
         {
             byte[] buffer = Guid.NewGuid().ToByteArray();
             return BitConverter.ToInt64(buffer, 0);
         }

    }
}
