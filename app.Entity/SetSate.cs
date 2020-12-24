using System;
using System.Collections.Generic;
using System.Text;

namespace getApp.Entity
{
    public class StockInfo
    {
        private int m_infoid;//
        private string m_productname;//产品名称
        private string m_model;//	型号
        private string m_productsn;//	批号
        private string m_brand;	//品牌
        private string m_packing;//封装
        private int m_quantity;	//数量
        private int m_outnumber;//出货量
        private int m_userID;//人员id
        private float m_price;	//销售单价
        private float m_cost;//成本价
        private float m_amount;//小计 m_price*m_quantity
        private string m_cycle;//货期
        private string m_remark;//备注

        public StockInfo() { }

        public int Infoid
        {
            get { return m_infoid; }
            set { m_infoid = value; }
        }

        public string Remark
        {
            get { return m_remark; }
            set { m_remark = value; }
        }

        public string ProductName
        {
            get { return m_productname; }
            set { m_productname = value; }
        }

        public string Model
        {
            get { return m_model; }
            set { m_model = value; }
        }

        public string Productsn
        {
            get { return m_productsn; }
            set { m_productsn = value; }
        }

        public string Brand
        {
            get { return m_brand; }
            set { m_brand = value; }
        }

        public string Packing
        {
            get { return m_packing; }
            set { m_packing = value; }
        }

        public string Cycle
        {
            get { return m_cycle; }
            set { m_cycle = value; }
        }

        public int Quantity
        {
            get { return m_quantity; }
            set { m_quantity = value; }
        }

        public int OutNumber
        {
            get { return m_outnumber; }
            set { m_outnumber = value; }
        }

        public int UserID
        {
            get { return m_userID; }
            set { m_userID = value; }
        }

        public float Price
        {
            get { return m_price; }
            set { m_price = value; }
        }

        public float Cost
        {
            get { return m_cost; }
            set { m_cost = value; }
        }

        public float Amount
        {
            get { return Price * Quantity; }
        }

    }
}
