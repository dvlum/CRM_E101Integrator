﻿using Erp.BO;
using Erp.Contracts;
using Erp.Proxy.BO;
using Ice.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Crm.Sdk.Messages;



using System.ServiceModel;
using System.IdentityModel.Tokens;
using System.ServiceModel.Security;
using System.IO;
using System.Configuration;
using Ice.Proxy.BO;
using Ice.Contracts;
using Ice.BO;
using Ice.Lib.Framework;
using System.Collections;
using Microsoft.Xrm.Sdk.Messages;

namespace CRM_iE10IntegratorConsole
{
    class Program
    {
        static E10Manager epiConnector = new E10Manager();
        static E101Results epiResults = new E101Results();

        static DataSet ds = new DataSet();

        static void Main(string[] args)
        {          

            epiConnector.epiServer = ConfigurationManager.AppSettings["epiBinding"];
            epiConnector.epiConfig = ConfigurationManager.AppSettings["epiConfig"];
            epiConnector.epiUser = ConfigurationManager.AppSettings["epiUser"];
            epiConnector.epiPassword = ConfigurationManager.AppSettings["epiPass"];

            while (true)
            {
                try
                {


                    #region E101 Customers

                    Console.WriteLine("--------------------E10 Customers OPS---------------------------");
                    Console.WriteLine("Obtenemos los Cutomers pendientes de EPICOR...");
                    VerifyEpicorChange();
                    Console.WriteLine();

                    Thread.Sleep(2000);


                    #endregion

                    #region CRM Customers

                    Console.WriteLine("-------------------CRM Account OPS----------------------------");
                    Console.WriteLine("Obtenemos los Cutomers pendientes de CRM...");
                    VerifyCRMChange();
                    Console.WriteLine();

                    Thread.Sleep(2000);

                    #endregion


                    #region CRM Quotes

                    //Console.WriteLine("Obtenemos Quotes pendientes de CRM...");
                    //CRM_QuoteChange();

                    //Thread.Sleep(3000);

                    #endregion

                    #region E101 Contacts

                    Console.WriteLine("------------------E10 Contact OPS-----------------------------");
                    Console.WriteLine("Obtenemos Contactos pendientes de Epicor...");
                    E101CreateUpdateContact();
                    Console.WriteLine();
                    Thread.Sleep(2000);

                    #endregion

                    #region CRM Contacts                    

                    Console.WriteLine("------------------CRM Contacts OPS-----------------------------");
                    Console.WriteLine("Obtenemos Contactos pendientes de CRM...");
                    CRMCreateUpdateContact();
                    Console.WriteLine();
                    Thread.Sleep(2000);

                    #endregion
                    //Console.WriteLine("------------------E10 Quotes OPS-----------------------------");
                    //Console.WriteLine("Obtenemos las cotizaciones pendientes de Epicor...");
                    //E101CreateUpdCRMQuote();
                    //Console.WriteLine();

                    //Thread.Sleep(5000);
                    //DelectQuoteLine();
                   

                }
                catch (Exception Ex)
                {
                    Console.WriteLine(Ex.Message);
                }
            }
        }

        //---------------------- CRM CODE ----------------------//

        public static void VerifyCRMChange()
        {
            E101Results epiResults = new E101Results();

            try
            {
                #region Obtenemos las Operaciones del CRM

                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spCRMGetOps"].ToString());

                //Console.WriteLine("Obtenemos las Operaciones del CRM");
                DataTable dtCRMOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["crmConnection"], "Ops", 0);

                Hashtable hsCRMOps = new Hashtable();

                //Console.WriteLine("Total de Operaciones [" + dtCRMOps.Rows.Count.ToString() + "]");
                foreach (DataRow R in dtCRMOps.Rows)
                {
                    hsCRMOps.Add(R["Idx"].ToString(), R["AccountId"].ToString());
                    
                }
                #endregion

                #region Obtenemos las Operaciones que ya fueron Procesadas

                spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetCRMProcessedOps"].ToString());

                //Console.WriteLine("Obtenemos las Operaciones ya procesadas del CRM");
                DataTable dtE10ProcessedOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["iE101Connection"], "Ops", 0);

                Hashtable hsE10Ops = new Hashtable();

                //Console.WriteLine("Total de Operaciones procesadas [" + dtE10ProcessedOps.Rows.Count.ToString() + "]");
                foreach (DataRow R in dtE10ProcessedOps.Rows)
                {
                    hsE10Ops.Add(R["CRMID"].ToString(), R["CRMID"].ToString());
                    
                }

                #endregion

                #region Obtenemos las Operaciones reales a Procesar

                //Console.WriteLine("Obtenemos las Operaciones reales a Procesar...");
                Hashtable opsToProcess = CompareHashtables(hsCRMOps, hsE10Ops);

                #endregion

                Console.WriteLine("Operaciones reales a Procesar [" + opsToProcess.Count.ToString() + "]");
                foreach (DictionaryEntry entry in opsToProcess)
                {
                    Console.WriteLine("Hacemos la conexion con el CRM para procesar [" + entry.Key.ToString() + "]");
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    string acctName = string.Empty;


                    QueryExpression qry = new QueryExpression("account");
                    qry.ColumnSet = new ColumnSet(new string[] { "accountid", "accountnumber", "name"});
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = new ConditionExpression("accountid", ConditionOperator.Equal, entry.Value.ToString());
                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);                    

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    Console.WriteLine("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                    if (results != null & results.Entities.Count > 0)
                    {
                        Console.WriteLine("Account Id [" + results.Entities[0].Id.ToString() + "]");
                        acctName = results.Entities[0]["name"].ToString();
                        //Entity updAccount = new Entity("account", "accountid", Guid.Parse(results.Entities[0].Id.ToString()));

                        ColumnSet cols = new ColumnSet(new String[] { "epic6s_tempcustid", "accountnumber", "name", "address1_line1",
                            "address1_line2", "address1_line3", "address1_stateorprovince", "address1_city", "address1_postalcode",
                            "epic6s_countryid", "epic6s_customergroupid", "epic6s_termsid" });
                        Entity updAccount = clientConn.Retrieve("account", Guid.Parse(results.Entities[0].Id.ToString()), cols);

                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine(manageNullKey(updAccount, "epic6s_countryid"));
                        //Console.WriteLine(((EntityReference)updAccount["epic6s_countryid"]).Id.ToString());
                        Console.WriteLine(manageNullKey(updAccount, "epic6s_customergroupid"));
                        //Console.WriteLine(((EntityReference)updAccount["epic6s_customergroupid"]).Id.ToString());
                        Console.WriteLine(manageNullKey(updAccount, "epic6s_termsid"));
                        //Console.WriteLine(((EntityReference)updAccount["epic6s_termsid"]).Id.ToString());
                        Console.WriteLine("--------------------------------------------------");

                        #region Obtenemos los Ids de Terms, Groups y Country

                        ColumnSet colsGroups = null;
                        Entity groupsEntity = null;
                        ColumnSet colsCountry = null;
                        Entity countryEntity = null;
                        ColumnSet colsTerms = null;
                        Entity termsEntity = null;

                        Console.WriteLine("--------------------------------------------------");
                        if (manageNullKey(updAccount, "epic6s_termsid") != "NA")
                        {
                            colsTerms = new ColumnSet(new String[] { "epic6s_terms", "epic6s_termscode" });
                            termsEntity = clientConn.Retrieve("epic6s_terms", Guid.Parse(((EntityReference)updAccount["epic6s_termsid"]).Id.ToString()), colsTerms);
                            Console.WriteLine(manageNullKey(termsEntity, "epic6s_termscode"));
                        }

                        if (manageNullKey(updAccount, "epic6s_customergroupid") != "NA")
                        {
                            colsGroups = new ColumnSet(new String[] { "epic6s_custgroupdesc", "epic6s_groupcode" });
                            groupsEntity = clientConn.Retrieve("epic6s_customergroup", Guid.Parse(((EntityReference)updAccount["epic6s_customergroupid"]).Id.ToString()), colsGroups);
                            Console.WriteLine(manageNullKey(groupsEntity, "epic6s_groupcode"));
                        }

                        if (manageNullKey(updAccount, "epic6s_countryid") != "NA")
                        {
                            colsCountry = new ColumnSet(new String[] { "epic6s_countryname", "epic6s_countrynumber" });
                            countryEntity = clientConn.Retrieve("epic6s_country", Guid.Parse(((EntityReference)updAccount["epic6s_countryid"]).Id.ToString()), colsCountry);
                            Console.WriteLine(manageNullKey(countryEntity, "epic6s_countrynumber"));
                        }
                        Console.WriteLine("--------------------------------------------------");

                        #endregion

                        #region Creamos los datos para Actualizar en Epicor.

                        Console.WriteLine("Creamos los datos para Actualizar en Epicor.");
                        DataSet dsParams = new DataSet();

                        // Creamos la tabla del encabezado
                        DataTable dtHeader = new DataTable();
                        dtHeader.Columns.Add("Company", typeof(string));
                        dtHeader.Rows.Add("2000");

                        // Creamos la tabla de los detalles
                        DataTable dtDetail = new DataTable();
                        dtDetail.Columns.Add("CustID", typeof(string));
                        dtDetail.Columns.Add("tmpCustID", typeof(string));
                        dtDetail.Columns.Add("Name", typeof(string));
                        dtDetail.Columns.Add("Address1", typeof(string));
                        dtDetail.Columns.Add("Address2", typeof(string));
                        dtDetail.Columns.Add("Address3", typeof(string));
                        dtDetail.Columns.Add("State", typeof(string));
                        dtDetail.Columns.Add("City", typeof(string));
                        dtDetail.Columns.Add("FaxNum", typeof(string));
                        dtDetail.Columns.Add("TermsCode", typeof(string));
                        dtDetail.Columns.Add("TerritoryID", typeof(string));
                        dtDetail.Columns.Add("CRMAccountID_c", typeof(string));
                        dtDetail.Columns.Add("Zip", typeof(string));
                        dtDetail.Columns.Add("GroupCode", typeof(string));
                        dtDetail.Columns.Add("CountryNum", typeof(string));

                        Console.WriteLine("Mostramos los datos a actualizar...");
                       

                        dtDetail.Rows.Add(manageNullKey(updAccount, "accountnumber"),
                                            manageNullKey(updAccount, "epic6s_tempcustid"),
                                            manageNullKey(updAccount, "name"),
                                            manageNullKey(updAccount, "address1_line1"),
                                            manageNullKey(updAccount, "address1_line2"),
                                            manageNullKey(updAccount, "address1_line3"),
                                            manageNullKey(updAccount, "address1_stateorprovince"),
                                            manageNullKey(updAccount, "address1_city"),
                                            "1",
                                            (manageNullKey(updAccount, "epic6s_termsid") != "NA")? manageNullKey(termsEntity, "epic6s_termscode"):"N30",
                                            "DEFAULT",
                                            results.Entities[0].Id.ToString(),                                            
                                            manageNullKey(updAccount, "address1_postalcode"),
                                            (manageNullKey(updAccount, "epic6s_customergroupid") != "NA") ? manageNullKey(groupsEntity, "epic6s_groupcode") : "",
                                            (manageNullKey(updAccount, "epic6s_countryid") != "NA") ? manageNullKey(countryEntity, "epic6s_countrynumber") : "2");

                        dsParams.Tables.Add(dtHeader);
                        dsParams.Tables.Add(dtDetail);

                        Console.WriteLine("Ejecutamos el metodo de Epicor [AddUpdCustomer]");
                        AddUpdCustomer(epiConnector, dsParams);

                        #endregion


                        #region Actulaizamos Operacion como Exito

                        String spSetCrmSuccess = File.ReadAllText(ConfigurationManager.AppSettings["spSetCrmSuccess"].ToString());

                        LAVHGenericMethods.DB.EXECStoreProcedure(spSetCrmSuccess.Replace("#IDX#", entry.Key.ToString()), 
                            LAVHGenericMethods.DB.OpenConnection(ConfigurationManager.AppSettings["crmConnection"].ToString()));

                        
                        Hashtable hsUD01 = new Hashtable();
                        hsUD01.Add("Key1", DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                        hsUD01.Add("Key2", entry.Key.ToString());
                        hsUD01.Add("Key3", "CRM");
                        hsUD01.Add("Key4", "");
                        hsUD01.Add("Key5", "");
                        hsUD01.Add("CheckBox01", "true");

                        insertDataUD01(epiConnector, hsUD01);

                        #endregion
                    }
                }

                #region OLD CODE
                /*
                CrmServiceClient clientConn = new CrmServiceClient("ServiceUri=https://intcrm.2mybi.com/CRMDEV;" + // for prod use "ServiceUri=https://crm.2mybi.com/CRM;" 
                                                              "AuthType=IFD;Domain=ztrserve;" +
                                                              "UserName=sixspartner@ztr.biz;Password=Enter@ZTR;" +
                                                              "LoginPrompt=Never;");

                string acctName = string.Empty;


                QueryExpression qry = new QueryExpression("account");
                qry.ColumnSet = new ColumnSet(new string[] { "accountid", "accountnumber", "name", "systemuserid" });
                FilterExpression filter = new FilterExpression(LogicalOperator.And);
                ConditionExpression con = new ConditionExpression("accountid", ConditionOperator.Equal, "A592872A-2626-E711-80CC-005056A4467D");
                filter.Conditions.Add(con);
                qry.Criteria.AddFilter(filter);

                //QueryExpression qry = new QueryExpression("systemuser");
                //qry.ColumnSet = new ColumnSet(new string[] { "systemuserid" });
                //FilterExpression filter = new FilterExpression(LogicalOperator.And);
                //ConditionExpression con = new ConditionExpression("systemuserid", ConditionOperator.Equal, "b146f0fd-be5a-e411-80d2-005056be1c27");
                //filter.Conditions.Add(con);
                //qry.Criteria.AddFilter(filter);

                EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                if (results != null & results.Entities.Count > 0)
                {
                    Entity updAccount = new Entity("account", "accountid", Guid.Parse(results.Entities[0].Id.ToString()));


                    updAccount["name"] = "IVAN2"; // "AN20170322 Test 3 - " + DateTime.Now;

                    clientConn.Update(updAccount);
                }
                */
                //A592872A-2626-E711-80CC-005056A4467D
                #endregion

            }
            catch (Exception Ex)
            {
                Console.WriteLine(epiResults.TransactionData + "[" + Ex.Message + "]");
            }
        }

        public static void VerifyEpicorChange()
        {
            try
            {
                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetOps"].ToString());
                String spUpdOps = String.Empty;

                DataTable dtOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["iE101Connection"], "Ops", 0);

                Console.WriteLine("Number of Records to Process: ["+ dtOps.Rows.Count.ToString() +"]");

                foreach (DataRow R in dtOps.Rows)
                {
                    //connect to CRM
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    /*CrmServiceClient clientConn = new CrmServiceClient("ServiceUri=https://intcrm.2mybi.com/CRMDEV;" + // for prod use "ServiceUri=https://crm.2mybi.com/CRM;" 
                                                                 "AuthType=IFD;Domain=ztrserve;" +
                                                                 "UserName=sixspartner@ztr.biz;Password=Enter@ZTR;" +
                                                                 "LoginPrompt=Never;");*/


                    #region Verificamos si existe
                    string acctName = string.Empty;
                    QueryExpression qry = new QueryExpression("account");
                    qry.ColumnSet = new ColumnSet(new string[] { "accountid", "accountnumber", "name" });
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = null;

                    if (R["AccountID"].ToString() == "")
                        con = new ConditionExpression("accountnumber", ConditionOperator.Equal, R["CustNum"].ToString());
                    else
                        con = new ConditionExpression("accountid", ConditionOperator.Equal, R["AccountID"].ToString());

                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code                    

                    if (results != null & results.Entities.Count > 0)
                    {
                        Console.WriteLine("Update...");
                        acctName = results.Entities[0]["name"].ToString();
                        Console.WriteLine("Account Id a actualizar: [" + results.Entities[0].Id.ToString() + "]");
                        Entity updAccount = new Entity("account", "accountid", Guid.Parse(results.Entities[0].Id.ToString()));

                        //Name                        
                        updAccount["name"] = R["Name"].ToString(); // "AN20170322 Test 3 - " + DateTime.Now;
                        updAccount["accountnumber"] = R["CustNum"].ToString();
                        //City
                        updAccount["address1_line1"] = R["Address1"].ToString();
                        updAccount["address1_line2"] = R["Address2"].ToString();
                        updAccount["address1_line3"] = R["Address3"].ToString();
                        updAccount["address1_stateorprovince"] = R["State"].ToString();
                        updAccount["address1_city"] = R["City"].ToString();
                        updAccount["address1_postalcode"] = R["Zip"].ToString();

                        updAccount["epic6s_termsid"] = (R["TermsCode"].ToString() != "")? new EntityReference("epic6s_terms", Guid.Parse(R["TermsCode"].ToString())) : null; 
                        updAccount["epic6s_customergroupid"] = (R["GroupCode"].ToString() != "")? new EntityReference("epic6s_customergroup", Guid.Parse(R["GroupCode"].ToString())) : null; 
                        updAccount["epic6s_countryid"] = (R["CountryNum"].ToString() != "")? new EntityReference("epic6s_country", Guid.Parse(R["CountryNum"].ToString())) : null; 

                        updAccount["epic6s_updatedinepicor"] = true; 
                        clientConn.Update(updAccount);
                        Console.WriteLine("DONE!");
                    }
                    else
                    {
                        Console.WriteLine("CREATE!");
                        //another way to create a new record using entity object
                        Entity account = new Entity("account");

                        //Name
                        account["name"] = R["Name"].ToString(); // "AN20170322 Test 3 - " + DateTime.Now;

                        //City
                        account["address1_line1"] = R["Address1"].ToString();
                        account["address1_line2"] = R["Address2"].ToString();
                        account["address1_line3"] = R["Address3"].ToString();
                        account["address1_stateorprovince"] = R["State"].ToString();
                        account["address1_city"] = R["City"].ToString();
                        account["address1_postalcode"] = R["Zip"].ToString();
                        account["epic6s_updatedinepicor"] = true;

                        //accountnum
                        account["accountnumber"] = R["CustNum"].ToString();
                        //create the record - create method is available in latest version of tooling
                        Guid accId = clientConn.Create(account);

                        #region Actualizamos el Account ID en Epicor

                        DataSet dsParams = new DataSet();

                        // Creamos la tabla del encabezado
                        DataTable dtHeader = new DataTable();
                        dtHeader.Columns.Add("Company", typeof(string));
                        dtHeader.Rows.Add("2000");

                        // Creamos la tabla de los detalles
                        DataTable dtDetail = new DataTable();
                        dtDetail.Columns.Add("CustID", typeof(string));                        
                        dtDetail.Columns.Add("CRMAccountID_c", typeof(string));
                        dtDetail.Columns.Add("FaxNum", typeof(string));

                        Console.WriteLine("Mostramos los datos a actualizar...");


                        dtDetail.Rows.Add(R["CustNum"].ToString(),
                                            accId.ToString(),
                                            "1");

                        dsParams.Tables.Add(dtHeader);
                        dsParams.Tables.Add(dtDetail);

                        Console.WriteLine("Ejecutamos el metodo de Epicor [AddUpdCustomer]");
                        AddUpdCustomer(epiConnector, dsParams);

                        #endregion

                        Console.WriteLine("DONE!");

                        //accountnumber has to be an alternate key 
                        //Entity updAccount = new Entity("account", "accountnumber", "AAAAA3");
                        //Entity updAccount = new Entity("account", "accountid", accId);
                        //updAccount["address1_city"] = R["address1_city"].ToString();

                        //clientConn.Update(updAccount);



                    }

                    //spUpdOps = File.ReadAllText(ConfigurationManager.AppSettings["spUpdOps"].ToString());
                    //spUpdOps = spUpdOps.Replace("#IDX#", R["Idx"].ToString());

                    #region Actulaizamos Operacion como Exito

                    Hashtable hsUD01 = new Hashtable();
                    hsUD01.Add("Key1", R["Idx"].ToString());
                    hsUD01.Add("Key2", R["CustNum"].ToString());
                    hsUD01.Add("Key3", "E101");
                    hsUD01.Add("Key4", "");
                    hsUD01.Add("Key5", "");
                    hsUD01.Add("CheckBox01", "true");

                    insertDataUD01(epiConnector, hsUD01);

                    #endregion

                    //LAVHGenericMethods.DB.EXECStoreProcedure(spUpdOps,
                    //    LAVHGenericMethods.DB.OpenConnection(ConfigurationManager.AppSettings["iE101Connection"].ToString()));

                    #endregion


                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
            }
        }

        public static void CRM_QuoteChange()
        {
            E101Results epiResults = new E101Results();

            try
            {
                #region Obtenemos las Operaciones del CRM

                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetQuoteOps"].ToString());

                Console.WriteLine("Obtenemos las Operaciones del CRM");
                DataTable dtCRMOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["crmConnection"], "Ops", 0);
                

                Console.WriteLine("Total de Operaciones [" + dtCRMOps.Rows.Count.ToString() + "]");

                #endregion

                #region Procesamos las Cotizaciones

                foreach (DataRow R in dtCRMOps.Rows)
                {
                    String CRMQuoteID = R[0].ToString();

                    Console.WriteLine("Procesando Quote# [" + CRMQuoteID + "]");
                    Console.WriteLine("Hacemos la conexion con el CRM para procesar [" + CRMQuoteID + "]");

                    // Hacemos la conexion con el CRM
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    QueryExpression qry = new QueryExpression("quote");
                    qry.ColumnSet = new ColumnSet(new string[] { "quotenumber", "customerid", "new_customerpo", "ownerid", "description", "new_epicorquoteid", "quoteid", "epic6s_needby" });
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = new ConditionExpression("quotenumber", ConditionOperator.Equal, CRMQuoteID);
                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);
                    

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    Console.WriteLine("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                    foreach (Entity Q in results.Entities)
                    {
                        /*Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine(manageNullKey(Q, "quotenumber"));
                        Console.WriteLine(manageNullKey(Q, "customerid"));
                        Console.WriteLine(manageNullKey(Q, "new_customerpo"));
                        Console.WriteLine(manageNullKey(Q, "ownerid"));
                        Console.WriteLine(manageNullKey(Q, "description"));
                        Console.WriteLine("--------------------------------------------------");*/

                        ColumnSet cols = new ColumnSet(new String[] { "epic6s_tempcustid", "accountnumber", "name", "address1_line1", "address1_line2", "address1_line3", "address1_stateorprovince", "address1_city" });
                        Entity custAccount = clientConn.Retrieve("account", Guid.Parse(((EntityReference)Q["customerid"]).Id.ToString()), cols);

                        /*Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine(manageNullKey(custAccount, "name"));
                        Console.WriteLine(manageNullKey(custAccount, "accountnumber"));     // CustID Epicor
                        Console.WriteLine(((EntityReference)Q["customerid"]).Id.ToString());
                        Console.WriteLine(((EntityReference)Q["ownerid"]).Id.ToString());
                        Console.WriteLine("--------------------------------------------------");*/
                        Console.WriteLine(manageNullKey(Q, "epic6s_needby"));

                        #region Creamos Operacion en Epicor

                        DataSet dsParams = new DataSet();

                        DataTable dtQuoteHead = new DataTable();
                        dtQuoteHead.Columns.Add("Company", typeof(string));
                        dtQuoteHead.Columns.Add("CustID", typeof(string));
                        dtQuoteHead.Columns.Add("QuoteComment", typeof(string));
                        dtQuoteHead.Columns.Add("PONum", typeof(string));
                        dtQuoteHead.Columns.Add("QuoteNum", typeof(string));
                        dtQuoteHead.Columns.Add("NeedByDate", typeof(string)); 

                        dtQuoteHead.Rows.Add("2000",
                                                manageNullKey(custAccount, "accountnumber"),
                                                CRMQuoteID,
                                                manageNullKey(Q, "new_customerpo"),
                                                manageNullKey(Q, "new_epicorquoteid"),
                                                manageNullKey(Q, "epic6s_needby"));

                        DataTable dtQuoteDtl = new DataTable();
                        dtQuoteDtl.Columns.Add("PartNum", typeof(string));
                        dtQuoteDtl.Columns.Add("LineDesc", typeof(string));
                        dtQuoteDtl.Columns.Add("OrderQty", typeof(string));
                        dtQuoteDtl.Columns.Add("DocExpUnitPrice", typeof(string));

                        //----------------------------------
                        QueryExpression qryDtl = new QueryExpression("quotedetail");
                        qryDtl.ColumnSet = new ColumnSet(new string[] { "quoteid", "productid", "productdescription", "quantity", "priceperunit" });
                        FilterExpression filterDtl = new FilterExpression(LogicalOperator.And);
                        ConditionExpression conDtl = new ConditionExpression("quoteid", ConditionOperator.Equal, Q["quoteid"].ToString());
                        filterDtl.Conditions.Add(conDtl);
                        qryDtl.Criteria.AddFilter(filterDtl);

                        
                        EntityCollection resultsDtl = clientConn.RetrieveMultiple(qryDtl);  //adjust the CrmServiceClient client object to the name used in your code

                        Console.WriteLine("Quote Details: [" + resultsDtl.Entities.Count.ToString() + "]");
                        foreach (Entity QDTL in resultsDtl.Entities)
                        {
                            //name, productnumber, description
                            ColumnSet colsProd = new ColumnSet(new String[] { "name", "productnumber", "description" });
                            Entity prodEntity = clientConn.Retrieve("product", Guid.Parse(((EntityReference)QDTL["productid"]).Id.ToString()), colsProd);
                            Console.WriteLine("Line Guid: " + ((EntityReference)QDTL["productid"]).Id.ToString());
                            /*Console.WriteLine("--------------------------------------------------"); 
                            Console.WriteLine(manageNullKey(prodEntity, "productnumber"));
                            Console.WriteLine(manageNullKey(prodEntity, "name"));                            
                            Console.WriteLine(manageNullKey(QDTL, "quantity"));
                            Console.WriteLine(((Microsoft.Xrm.Sdk.Money)QDTL["priceperunit"]).Value.ToString());
                            Console.WriteLine("--------------------------------------------------");*/

                            dtQuoteDtl.Rows.Add(manageNullKey(prodEntity, "productnumber"), 
                                                manageNullKey(prodEntity, "name"),
                                                manageNullKey(QDTL, "quantity"),
                                                ((Microsoft.Xrm.Sdk.Money)QDTL["priceperunit"]).Value.ToString());
                        }
                        //QuoteDetail - productid - description - quantity - priceperunit
                        //----------------------------------

                        dsParams.Tables.Add(dtQuoteHead);
                        dsParams.Tables.Add(dtQuoteDtl);

                        Console.WriteLine("Ejecutamos el metodo de Epicor [CreateNewQuote]");
                        epiResults = CreateNewQuote(epiConnector, dsParams);

                        Console.WriteLine("Epicor Quote Number: " + epiResults.EpicorNum.ToString());
                        Console.WriteLine("EXITO [CreateNewQuote]");

                        Console.WriteLine("Updating CRM transaction...");                       

                        Q["new_epicorquoteid"] = epiResults.EpicorNum.ToString();
                        Q["epic6s_updatedinepicor"] = true;
                        clientConn.Update(Q);

                        Console.WriteLine("CRM Update complete.");
                        Console.WriteLine("Set CRM Op Success.");
                        String spSetCrmSuccess = File.ReadAllText(ConfigurationManager.AppSettings["spSetQuoteOPSuccess"].ToString());
                        LAVHGenericMethods.DB.EXECStoreProcedure(spSetCrmSuccess.Replace("#CRMQUOTENUM#", CRMQuoteID),
                            LAVHGenericMethods.DB.OpenConnection(ConfigurationManager.AppSettings["crmConnection"].ToString()));
                        Console.WriteLine("DONE!");

                        #endregion


                    }

                    Thread.Sleep(6000);
                }

                #endregion    

            }
            catch (Exception Ex)
            {
                Console.WriteLine(epiResults.TransactionData + "[" + Ex.Message + "]");
            }
        }

        public static void CRMCreateUpdateContact()
        {
            try
            {
                #region Obtenemos las Operaciones del CRM

                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetCRMContactOps"].ToString());

                Console.WriteLine("Obtenemos los contactos por actualizar del CRM...");
                DataTable dtCRMOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["crmConnection"], "Ops", 0);

                Console.WriteLine("Total de Operaciones [" + dtCRMOps.Rows.Count.ToString() + "]");

                foreach (DataRow R in dtCRMOps.Rows)
                {
                    String CRMContactID = R["ContactReference"].ToString();

                    Console.WriteLine("Procesando Contacto# [" + CRMContactID + "]");
                    Console.WriteLine("Hacemos la conexion con el CRM para procesar [" + CRMContactID + "]");

                    // Hacemos la conexion con el CRM
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    QueryExpression qry = new QueryExpression("contact");
                    qry.ColumnSet = new ColumnSet(new string[] { "contactid", "epic6s_epicorcontid", "telephone1", "mobilephone", "firstname", "lastname", "parentcustomerid", "emailaddress1" });
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = new ConditionExpression("contactid", ConditionOperator.Equal, CRMContactID);
                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    Console.WriteLine("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                    foreach (Entity Q in results.Entities)
                    {
                        ColumnSet cols = new ColumnSet(new String[] { "epic6s_tempcustid", "accountnumber", "name", "address1_line1", "address1_line2", "address1_line3", "address1_stateorprovince", "address1_city" });
                        Entity custAccount = clientConn.Retrieve("account", Guid.Parse(((EntityReference)Q["parentcustomerid"]).Id.ToString()), cols);

                        /*Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine(manageNullKey(Q, "epic6s_epicorcontid"));
                        Console.WriteLine(manageNullKey(Q, "telephone1"));
                        Console.WriteLine(manageNullKey(Q, "mobilephone"));
                        Console.WriteLine(manageNullKey(Q, "firstname"));
                        Console.WriteLine(manageNullKey(Q, "lastname"));
                        Console.WriteLine(manageNullKey(custAccount, "accountnumber"));
                        Console.WriteLine(manageNullKey(Q, "emailaddress1"));
                        Console.WriteLine("--------------------------------------------------");*/

                        #region Creamos los datos para Actualizar en Epicor.

                        Console.WriteLine("Creamos los datos para Actualizar en Epicor.");
                        DataSet dsParams = new DataSet();

                        // Creamos la tabla del encabezado
                        DataTable dtHeader = new DataTable();
                        dtHeader.Columns.Add("Company", typeof(string));
                        dtHeader.Columns.Add("CustID", typeof(string));
                        dtHeader.Columns.Add("PhoneNum", typeof(string));
                        dtHeader.Columns.Add("CellPhoneNum", typeof(string));
                        dtHeader.Columns.Add("Name", typeof(string));
                        dtHeader.Columns.Add("EMailAddress", typeof(string));
                        dtHeader.Columns.Add("ConNum", typeof(string));
                        dtHeader.Columns.Add("Comment", typeof(string));  // Se guarda temporalmente el GUID de CRM


                        dtHeader.Rows.Add("2000",
                                            manageNullKey(custAccount, "accountnumber"),
                                            manageNullKey(Q, "telephone1"),
                                            manageNullKey(Q, "mobilephone"),
                                            manageNullKey(Q, "firstname") + " " + manageNullKey(Q, "lastname"),
                                            manageNullKey(Q, "emailaddress1"),
                                            manageNullKey(Q, "epic6s_epicorcontid"),
                                            CRMContactID);

                        dsParams.Tables.Add(dtHeader);                        

                        Console.WriteLine("Ejecutamos el metodo de Epicor [AddUpdtContact]");
                        try
                        {
                            AddUpdtContact(epiConnector, dsParams);
                            Console.WriteLine("DONE!");
                            if (manageNullKey(Q, "epic6s_epicorcontid") == "NA")
                            {
                                Console.WriteLine("Enviamos No. de contacto a CRM...");
                                E101CreateUpdateContact();
                            }

                            Console.WriteLine("Set CRM Op Success.");
                            String spSetCrmSuccess = File.ReadAllText(ConfigurationManager.AppSettings["spSetContactOPSuccess"].ToString());
                            LAVHGenericMethods.DB.EXECStoreProcedure(spSetCrmSuccess.Replace("#IDX#", R["Idx"].ToString()),
                                LAVHGenericMethods.DB.OpenConnection(ConfigurationManager.AppSettings["crmConnection"].ToString()));
                            Console.WriteLine("DONE!");
                            //spSetContactOPSuccess
                        }
                        catch(Exception Ex)
                        {
                            Console.WriteLine(Ex.Message);
                        }

                        #endregion

                    }
                }

                    #endregion
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
            }
        }


        //---------------------- CRM CODE ----------------------//

        public static Hashtable CompareHashtables(Hashtable ht1, Hashtable ht2)
        {
            Hashtable resultsOfCompare = new Hashtable();

            foreach (DictionaryEntry entry in ht1)
            {
                if (!(ht2.ContainsKey(entry.Key)))
                {
                    resultsOfCompare.Add(entry.Key, entry.Value);
                }
            }
            return resultsOfCompare;
        }

        public static string manageNullKey(Entity entity, string keyToVerify)
        {
            try
            {
                
                return (entity.Contains(keyToVerify) ? entity[keyToVerify].ToString() : "NA");
                
            }
            catch(Exception Ex)
            {
                return "NA";
            }
        }

        public static void UpdateCRMAccount(Guid accountID, string tpmCustID)
        {
            try
            {
                CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());
                
                ColumnSet cols = new ColumnSet(new String[] { "name" });
                Entity custAccount = clientConn.Retrieve("account", accountID, cols);

                custAccount["epic6s_tempcustid"] = tpmCustID;
                custAccount["epic6s_updatedinepicor"] = true;
                clientConn.Update(custAccount);
            }
            catch(Exception Ex)
            {
                Console.WriteLine(Ex.Message);
            }
        }

        public static void DeleteQuoteLine()
        {
            try
            {
                CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());
                                
                QueryExpression qry = new QueryExpression("quote");
                qry.ColumnSet = new ColumnSet(new string[] { "quotenumber", "customerid", "new_customerpo", "ownerid", "description", "new_epicorquoteid", "quoteid", "epic6s_needby" });
                FilterExpression filter = new FilterExpression(LogicalOperator.And);
                ConditionExpression con = new ConditionExpression("quotenumber", ConditionOperator.Equal, "QUO-34047-V8Y9F2");
                filter.Conditions.Add(con);
                qry.Criteria.AddFilter(filter);

                EntityCollection results = clientConn.RetrieveMultiple(qry);

                foreach (Entity Q in results.Entities)
                {

                    QueryExpression qryDtl = new QueryExpression("quotedetail");
                    qryDtl.ColumnSet = new ColumnSet(new string[] { "quoteid", "productid", "productdescription", "quantity", "priceperunit", "quotedetailid" });
                    FilterExpression filterDtl = new FilterExpression(LogicalOperator.And);
                    ConditionExpression conDtl = new ConditionExpression("quoteid", ConditionOperator.Equal, Q["quoteid"].ToString());
                    filterDtl.Conditions.Add(conDtl);
                    qryDtl.Criteria.AddFilter(filterDtl);


                    EntityCollection resultsDtl = clientConn.RetrieveMultiple(qryDtl);  //adjust the CrmServiceClient client object to the name used in your code

                    Console.WriteLine("Quote Details: [" + resultsDtl.Entities.Count.ToString() + "]");
                    foreach (Entity QDTL in resultsDtl.Entities)
                    {
                        //name, productnumber, description
                        ColumnSet colsProd = new ColumnSet(new String[] { "name", "productnumber", "description" });
                        Entity prodEntity = clientConn.Retrieve("product", Guid.Parse(((EntityReference)QDTL["productid"]).Id.ToString()), colsProd);
                        Console.WriteLine("Line Guid to DEL: " + QDTL["quotedetailid"].ToString());

                        clientConn.Delete("quotedetail", Guid.Parse(QDTL["quotedetailid"].ToString()));
                        
                        Console.WriteLine("DEL SUCCESS");
                    }
                    clientConn.Update(Q);
                }
                //b147d7ee-e86e-e411-80d3-005056be1c27
                //b147d7ee-e86e-e411-80d3-005056be1c27
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
            }
        }
        
        public static void E101CreateUpdateContact()
        {
            try
            {
                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetContacOps"].ToString());
                String spUpdOps = String.Empty;

                DataTable dtOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["iE101Connection"], "Ops", 0);

                CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                Console.WriteLine("Number of Records to Process: [" + dtOps.Rows.Count.ToString() + "]");

                foreach (DataRow R in dtOps.Rows)
                {
                    string acctName = string.Empty;
                    QueryExpression qry = new QueryExpression("contact");
                    qry.ColumnSet = new ColumnSet(new string[] { "contactid", "epic6s_epicorcontid" });
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = null;

                    if(R["CRMcontactID"].ToString() == "")
                        con = new ConditionExpression("epic6s_epicorcontid", ConditionOperator.Equal, R["PerConID"].ToString());
                    else
                        con = new ConditionExpression("contactid", ConditionOperator.Equal, R["CRMcontactID"].ToString());

                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    if (results != null & results.Entities.Count > 0)
                    {
                        Console.WriteLine("Update...");                        
                        Console.WriteLine("Contact Id a actualizar: [" + results.Entities[0].Id.ToString() + "]");
                        Entity contact = new Entity("contact", "contactid", Guid.Parse(results.Entities[0].Id.ToString()));

                        
                        contact["epic6s_epicorcontid"] = int.Parse(R["PerConID"].ToString());
                        contact["telephone1"] = R["PhoneNum"].ToString(); // "CustCnt.PhoneNum";
                        contact["mobilephone"] = R["CellPhoneNum"].ToString(); //"CustCnt.CellPhoneNum";
                        contact["firstname"] = (R["Name"].ToString().Split(' ').Length > 1) ? R["Name"].ToString().Split(' ')[0] : R["Name"].ToString();  //"CustCnt.Name";
                        contact["lastname"] = (R["Name"].ToString().Split(' ').Length > 1) ? R["Name"].ToString().Split(' ')[1] : R["Name"].ToString();
                        contact["parentcustomerid"] = new EntityReference("account", Guid.Parse(R["CRMAccountID"].ToString()));
                        contact["emailaddress1"] = R["EmailAddress"].ToString(); 
                        contact["epic6s_updatedinepicor"] = true;

                        //create the record - create method is available in latest version of tooling
                        clientConn.Update(contact);
                        Console.WriteLine("DONE!");
                    }
                    else
                    {
                        Console.WriteLine("Creating Contact!");
                        //another way to create a new record using entity object
                        Entity contact = new Entity("contact");

                        contact["epic6s_epicorcontid"] = int.Parse(R["PerConID"].ToString());
                        contact["telephone1"] = R["PhoneNum"].ToString(); // "CustCnt.PhoneNum";
                        contact["mobilephone"] = R["CellPhoneNum"].ToString(); //"CustCnt.CellPhoneNum";
                        contact["firstname"] = (R["Name"].ToString().Split(' ').Length > 1) ? R["Name"].ToString().Split(' ')[0] : R["Name"].ToString();  //"CustCnt.Name";
                        contact["lastname"] = (R["Name"].ToString().Split(' ').Length > 1) ? R["Name"].ToString().Split(' ')[1] : R["Name"].ToString();
                        contact["parentcustomerid"] = new EntityReference("account", Guid.Parse(R["CRMAccountID"].ToString()));
                        contact["emailaddress1"] = R["EmailAddress"].ToString();
                        contact["epic6s_updatedinepicor"] = true;

                        //create the record - create method is available in latest version of tooling
                        Guid accId = clientConn.Create(contact);
                        Console.WriteLine("Contact ID: " + accId.ToString());
                    }

                    #region Actulaizamos Operacion como Exito

                    Hashtable hsUD01 = new Hashtable();
                    hsUD01.Add("Key1", R["Idx"].ToString());
                    hsUD01.Add("Key2", R["PerConID"].ToString());
                    hsUD01.Add("Key3", "E101CustContact");
                    hsUD01.Add("Key4", "");
                    hsUD01.Add("Key5", "");
                    hsUD01.Add("CheckBox01", "true");

                    insertDataUD01(epiConnector, hsUD01);

                    #endregion
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
            }
        }

        public static void E101CreateUpdCRMQuote()
        {
            #region //create a quote for this customer
            CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

            try
            {
                //create a batch for commit/rollback scenario
                Console.WriteLine("Creamos Batch...");
                Guid batchID = clientConn.CreateBatchOperationRequest("quotebatch", true, false);

                //create quote for this account
                //Entity quote = new Entity("quote");

                Dictionary<string, CrmDataTypeWrapper> quoteArray = new Dictionary<string, CrmDataTypeWrapper>();

                //set the customer
                //quoteArray["customerid"] = new CrmDataTypeWrapper(new EntityReference ("account", accId), CrmFieldType.Customer);

                Console.WriteLine("Obtenemos Id Cliente...");
                CrmDataTypeWrapper cust = new CrmDataTypeWrapper();
                cust.ReferencedEntity = "account";
                cust.Value = Guid.Parse("0C7A756E-A349-E711-80CE-005056A4467D"); // Hernandez quote
                cust.Type = CrmFieldType.Customer;

                quoteArray["customerid"] = cust;// new CrmDataTypeWrapper();

                //set the name
                quoteArray["name"] = new CrmDataTypeWrapper("Sample quote " + DateTime.Now.ToString(), CrmFieldType.String);

                //set the name
                Guid quoteID = Guid.NewGuid();
                quoteArray["quoteid"] = new CrmDataTypeWrapper(quoteID, CrmFieldType.UniqueIdentifier);

                //set the pricelist // change it to one of your records
                CrmDataTypeWrapper pricelevel = new CrmDataTypeWrapper();
                pricelevel.ReferencedEntity = "pricelevel";
                pricelevel.Value = new Guid("D1DDBE2B-B44F-E711-80DE-000D3AF32500");
                pricelevel.Type = CrmFieldType.Lookup;
                quoteArray["pricelevelid"] = pricelevel; //new CrmDataTypeWrapper(new EntityReference("pricelevel", new Guid("9222A75A-743D-E711-80E4-3863BB34E918")), CrmFieldType.Lookup);


                Console.WriteLine("Creamos Encabezado...");
                clientConn.CreateNewRecord("quote", quoteArray, batchId: batchID);


                Console.WriteLine("Cargamos datos de los detalles...");
                Dictionary<string, CrmDataTypeWrapper> detailArray = new Dictionary<string, CrmDataTypeWrapper>();

                //set write-in
                CrmDataTypeWrapper quote = new CrmDataTypeWrapper();
                quote.ReferencedEntity = "quote";
                quote.Value = quoteID;//new Guid("9222A75A-743D-E711-80E4-3863BB34E918");
                quote.Type = CrmFieldType.Lookup;

                detailArray["quoteid"] = quote;

                //set write-in
                //detailArray["isproductoverriden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                detailArray["isproductoverridden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                //set price overridden
                //detailArray["ispriceoverriden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                detailArray["ispriceoverridden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                //set the description
                detailArray["productdescription"] = new CrmDataTypeWrapper("write-in", CrmFieldType.String);

                //set the price
                detailArray["priceperunit"] = new CrmDataTypeWrapper((decimal)(15.5), CrmFieldType.CrmMoney);

                //qty
                detailArray["quantity"] = new CrmDataTypeWrapper((decimal)(10), CrmFieldType.CrmDecimal);

                Console.WriteLine("Creamos Detalle...");
                clientConn.CreateNewRecord("quotedetail", detailArray, batchId: batchID);

                //execute batch
                ExecuteMultipleResponse response = clientConn.ExecuteBatch(batchID);
                Console.WriteLine("Exito!");

            }
            catch (Exception Ex)
            {
                Console.WriteLine(Ex.Message);
            }
            #endregion
        }


        //-------------------- EPICOR CODE --------------------//

        public struct E10Manager
        {
            public String epiServer;
            public String epiConfig;
            public String epiUser;
            public String epiPassword;
        }

        public struct E101Results
        {            
            public String EpicorID;
            public Int32 EpicorNum;
            public String TransactionData;
            public String TransactionType;
        }

        public static E101Results AddUpdCustomer(E10Manager conf, DataSet dsParam)
        {
            E101Results epiResults = new E101Results();
            StringBuilder sbMessageOut = new StringBuilder();

            try
            {
                Boolean newRow = false;
                string tmpCustID = string.Empty;

                using (Session session = new Session(conf.epiUser, conf.epiPassword, conf.epiServer, Session.LicenseType.Default, conf.epiConfig))
                {
                    session.CompanyID = dsParam.Tables[0].Rows[0][0].ToString();

                    CustomerImpl custImpl =
                        Ice.Lib.Framework.WCFServiceSupport.CreateImpl<CustomerImpl>(session, Epicor.ServiceModel.Channels.ImplBase<CustomerSvcContract>.UriPath);

                    CustomerDataSet dsCustImpl = new CustomerDataSet();

                    try
                    {                        
                        string custID = (dsParam.Tables[1].Rows[0]["CustID"].ToString() != "NA") ? dsParam.Tables[1].Rows[0]["CustID"].ToString() : dsParam.Tables[1].Rows[0]["tmpCustID"].ToString();
                        Console.WriteLine("Buscamos: [" + custID + "]");
                        dsCustImpl = custImpl.GetByCustID(custID, false);
                        newRow = false;
                    }
                    catch (Exception Ex)
                    {
                        sbMessageOut.AppendLine("No se encontro el registro [" + Ex.Message + "]");
                        newRow = true;
                    }

                    if (newRow)
                    {
                        sbMessageOut.AppendLine("Creamos nuevo registro...");
                        custImpl.GetNewCustomer(dsCustImpl);
                    }
                    else
                    {
                        sbMessageOut.AppendLine("Actualizamos registro existente...");
                    }


                    dsCustImpl.Tables["Customer"].Rows[0].BeginEdit();

                    foreach (DataColumn C in dsParam.Tables[1].Columns)
                    {
                        if (C.ColumnName == "tmpCustID")
                        {
                            if (dsParam.Tables[1].Rows[0]["CustID"].ToString() == "NA" && dsParam.Tables[1].Rows[0]["tmpCustID"].ToString() == "NA")
                            {
                                tmpCustID = "TMP" + Guid.NewGuid().ToString("N").Substring(0, 7);
                                dsCustImpl.Tables["Customer"].Rows[0]["CustID"] = tmpCustID;
                                UpdateCRMAccount(Guid.Parse(dsParam.Tables[1].Rows[0]["CRMAccountID_c"].ToString()), tmpCustID);

                                Console.WriteLine("tmpCustID = NA so: " + tmpCustID);
                            }else
                            {
                                if (dsParam.Tables[1].Rows[0]["CustID"].ToString() == "NA")
                                    dsCustImpl.Tables["Customer"].Rows[0]["CustID"] = dsParam.Tables[1].Rows[0][C.ColumnName];
                            }
                        }
                        else
                        {
                            if (!(C.ColumnName == "CustID" && dsParam.Tables[1].Rows[0]["CustID"].ToString() == "NA" && dsCustImpl.Tables["Customer"].Rows[0][C.ColumnName].ToString() != ""))
                            {
                                dsCustImpl.Tables["Customer"].Rows[0][C.ColumnName] = dsParam.Tables[1].Rows[0][C.ColumnName];
                                //Console.WriteLine("CustID = NA so: " + dsCustImpl.Tables["Customer"].Rows[0]["CustID"].ToString());
                            }
                        }
                    }

                    dsCustImpl.Tables["Customer"].Rows[0].EndEdit();

                    custImpl.Update(dsCustImpl);

                    epiResults.EpicorID = "CustNum";
                    epiResults.EpicorNum = int.Parse(dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString());
                }

                epiResults.TransactionData = sbMessageOut.ToString();
                epiResults.TransactionType = "ADD|UPD";

                return epiResults;
            }
            catch (Exception Ex)
            {
                throw new Exception(Ex.Message + "[ " + sbMessageOut.ToString() + " ]");
            }
        }

        public static void insertDataUD01(E10Manager conf, Hashtable rowData)
        {
            try
            {
                using (Session session = new Session(conf.epiUser, conf.epiPassword, conf.epiServer, Session.LicenseType.Default, conf.epiConfig))
                {
                    UD01Impl implUD01 = WCFServiceSupport.CreateImpl<UD01Impl>(session, Epicor.ServiceModel.Channels.ImplBase<UD01SvcContract>.UriPath);
                    UD01DataSet dsUD01 = new UD01DataSet();

                    Int32 rowIndex = 0;
                    Boolean newRow = false;
                    StringBuilder sbLogMessage = new StringBuilder();

                    try
                    {
                        dsUD01 = implUD01.GetByID(rowData["Key1"].ToString(), rowData["Key2"].ToString(),
                                                    rowData["Key3"].ToString(), rowData["Key4"].ToString(),
                                                    rowData["Key5"].ToString());
                        newRow = false;
                    }
                    catch (Exception Ex)
                    {
                        sbLogMessage.AppendLine("No se encontro el registro [" + Ex.Message + "]");
                        newRow = true;
                    }

                    if (newRow)
                    {
                        sbLogMessage.AppendLine("Creamos nuevo registro...");
                        implUD01.GetaNewUD01(dsUD01);
                    }
                    else
                    {
                        sbLogMessage.AppendLine("Actualizamos registro existente...");
                        rowIndex = dsUD01.Tables["UD01"].Rows.Count - 1;
                    }

                    dsUD01.Tables["UD01"].Rows[0].BeginEdit();
                    foreach (String colName in rowData.Keys)
                    {
                        dsUD01.Tables["UD01"].Rows[0][colName] = rowData[colName];
                    }
                    dsUD01.Tables["UD01"].Rows[0].EndEdit();

                    implUD01.Update(dsUD01);

                   
                }
            }
            catch (Exception Ex)
            {
                throw new Exception(Ex.Message);
            }
        }

        public static E101Results CreateNewQuote(E10Manager conf, DataSet dsParams)
        {
            try
            {
                E101Results epiResults = new E101Results();
                StringBuilder sbMessageOut = new StringBuilder();
                bool newQuote = true;

                Console.WriteLine("Hacemos Conexion con Epicor...");

                using (Session session = new Session(conf.epiUser, conf.epiPassword, conf.epiServer, Session.LicenseType.Default, conf.epiConfig))
                {
                    Console.WriteLine("Exito al hacer la conexion");
                    session.CompanyID = dsParams.Tables[0].Rows[0]["Company"].ToString();
                    //session.PlantID = dsParams.Tables["RMAHead"].Rows[0]["Plant"].ToString();

                    QuoteImpl quoteImpl =
                        Ice.Lib.Framework.WCFServiceSupport.CreateImpl<QuoteImpl>(session, Epicor.ServiceModel.Channels.ImplBase<QuoteSvcContract>.UriPath);

                    QuoteDataSet dsQuoteImpl = new QuoteDataSet();

                    #region Verificamos si existe el Quote

                    try
                    {
                        Console.WriteLine("Looking for Quote...");
                        int quoteNum = (dsParams.Tables[0].Rows[0]["QuoteNum"].ToString() != "NA") ? int.Parse(dsParams.Tables[0].Rows[0]["QuoteNum"].ToString()) : 0;
                        dsQuoteImpl = quoteImpl.GetByID(quoteNum);
                        Console.WriteLine("Quote already exists, Updating...");
                        newQuote = false;
                    }
                    catch (Exception Ex)
                    {
                        Console.WriteLine("Quote Not Found...");
                        newQuote = true;
                    }
                    #endregion

                    #region Obtenemos el Encabezado

                    // Creamos las bases para la Cotizacion
                    if (newQuote)
                    {
                        Console.WriteLine("Creating Quote Header...");
                        quoteImpl.GetNewQuoteHed(dsQuoteImpl);
                    }
                    Console.WriteLine("CustID: " + dsParams.Tables[0].Rows[0]["CustID"].ToString());

                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["CustomerCustID"] = dsParams.Tables[0].Rows[0]["CustID"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                    quoteImpl.GetCustomerInfo(dsQuoteImpl);

                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["CustomerCustID"] = dsParams.Tables[0].Rows[0]["CustID"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["QuoteComment"] = dsParams.Tables[0].Rows[0]["QuoteComment"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["PONum"] = dsParams.Tables[0].Rows[0]["PONum"].ToString();
                    //dsQuoteImpl.Tables["QuoteHed"].Rows[0]["NeedByDate"] = dsParams.Tables[0].Rows[0]["NeedByDate"].ToString(); 
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                    Console.WriteLine("Updating...");
                    quoteImpl.Update(dsQuoteImpl);
                    Console.WriteLine("Success");

                    #endregion
                    
                    int _QuoteNum = int.Parse(dsQuoteImpl.Tables["QuoteHed"].Rows[0]["QuoteNum"].ToString());
                    epiResults.EpicorNum = _QuoteNum;

                    #region Borramos todas las lineas si existen
                    //Marcamos todas las lineas como Borrables
                    for (int i = 0; i < dsQuoteImpl.Tables["QuoteDtl"].Rows.Count; i++)
                    {
                        sbMessageOut.AppendLine("Borramos linea: [" + dsQuoteImpl.Tables["QuoteDtl"].Rows[i]["QuoteLine"].ToString() + "]");
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[i].Delete();
                    }
                    for (int i = 0; i < dsQuoteImpl.Tables["QuoteCoPart"].Rows.Count; i++)
                    {
                        dsQuoteImpl.Tables["QuoteCoPart"].Rows[i].Delete();
                    }
                    for (int i = 0; i < dsQuoteImpl.Tables["QuoteQty"].Rows.Count; i++)
                    {
                        dsQuoteImpl.Tables["QuoteQty"].Rows[i].Delete();
                    }

                    quoteImpl.Update(dsQuoteImpl);
                    #endregion

                    #region Creamos las Lineas

                    //productid - description - quantity - priceperunit
                   

                    string partNum = "";
                    bool llsPhantom = false;
                    bool lIsSalesKit = false;
                    bool salesKitView = false;
                    bool removeKitComponents = false;
                    bool suppressUserPrompts = false;
                    bool runChkPrePartInfo = true;
                    string vMessage = String.Empty;
                    string vPMessage = String.Empty;
                    string vBMessage = String.Empty;
                    string uomCode = String.Empty;
                    string rowType = String.Empty;
                    bool vSubAvail = false;
                    string vMsgType = String.Empty;
                    bool getPartXRefInfo = true;
                    bool checkChangeKitParent = true;
                    bool multipleMatch = false;
                    bool promptToExplodeBOM = false;
                    string cDeleteComponentsMessage = String.Empty;
                    string explodeBOMerrMessage = String.Empty;
                    int rowIndex = 0;

                    foreach (DataRow quoteLine in dsParams.Tables[1].Rows)
                    {
                        partNum = quoteLine["PartNum"].ToString();

                        quoteImpl.GetNewQuoteDtl(dsQuoteImpl, _QuoteNum);

                        quoteImpl.ChangePartNumMaster(ref partNum, ref llsPhantom, ref lIsSalesKit, ref uomCode,
                                                        rowType, Guid.NewGuid(), salesKitView, removeKitComponents,
                                                        suppressUserPrompts, runChkPrePartInfo, out vMessage,
                                                        out vPMessage, out vBMessage, out vSubAvail, out vMsgType,
                                                        getPartXRefInfo, checkChangeKitParent, out cDeleteComponentsMessage,
                                                        out multipleMatch, out promptToExplodeBOM, out explodeBOMerrMessage,
                                                        dsQuoteImpl);

                        bool lSubstitutePartsExist = false;
                        rowIndex = dsQuoteImpl.Tables["QuoteDtl"].Rows.Count - 1;
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        quoteImpl.ChangePartNum(dsQuoteImpl, lSubstitutePartsExist, "");

                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        quoteImpl.Update(dsQuoteImpl);

                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        quoteImpl.GetDtlUnitPriceInfo_User(true, true, false, true, dsQuoteImpl);

                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        quoteImpl.Update(dsQuoteImpl);
                    }
                    #endregion
                }

                return epiResults;
            }
            catch (Exception Ex)
            {
                throw new Exception(Ex.Message);
            }
        }

        public static E101Results AddUpdtContact(E10Manager conf, DataSet dsParams)
        {
            try
            {
                E101Results epiResults = new E101Results();
                StringBuilder sbMessageOut = new StringBuilder();
                bool newContact = true;

                Console.WriteLine("Hacemos Conexion con Epicor...");

                using (Session session = new Session(conf.epiUser, conf.epiPassword, conf.epiServer, Session.LicenseType.Default, conf.epiConfig))
                {
                    Console.WriteLine("Exito al hacer la conexion");
                    session.CompanyID = dsParams.Tables[0].Rows[0]["Company"].ToString();
                    //session.PlantID = dsParams.Tables["RMAHead"].Rows[0]["Plant"].ToString();                    

                    CustomerImpl custImpl =
                        Ice.Lib.Framework.WCFServiceSupport.CreateImpl<CustomerImpl>(session, Epicor.ServiceModel.Channels.ImplBase<CustomerSvcContract>.UriPath);

                    CustomerDataSet dsCustImpl = new CustomerDataSet();

                    try
                    {                        
                        Console.WriteLine("Buscamos: [" + dsParams.Tables[0].Rows[0]["custID"].ToString() + "]"); 
                        dsCustImpl = custImpl.GetByCustID(dsParams.Tables[0].Rows[0]["custID"].ToString(), false);                        
                    }
                    catch (Exception Ex)
                    {
                        throw new Exception("No se encontro el registro [" + Ex.Message + "]");                        
                    }

                    CustCntImpl custCntImpl =
                        Ice.Lib.Framework.WCFServiceSupport.CreateImpl<CustCntImpl>(session, Epicor.ServiceModel.Channels.ImplBase<CustCntSvcContract>.UriPath);

                    CustCntDataSet dsCustCntImpl = new CustCntDataSet();
                    
                    try
                    {
                        Console.WriteLine("Buscamos el contacto No. " + dsParams.Tables[0].Rows[0]["ConNum"].ToString() +
                            " del Cliente No. " + dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString());

                        if (dsParams.Tables[0].Rows[0]["ConNum"].ToString() != "NA")
                        {
                            bool morePages = false;
                            dsCustCntImpl = custCntImpl.GetRows("CustNum = '" + dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString() + "' BY Name", "", 0, 0, out morePages);
                            var rowCount = (from result1 in dsCustCntImpl.Tables["CustCnt"].AsEnumerable()
                                            where result1.Field<int>("PerConID") == int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()) 
                                            select result1).Count();

                            Console.WriteLine("Row Count: " + rowCount.ToString());
                            if (rowCount > 0)
                                newContact = false;
                            else
                                newContact = true;

                            //perConImpl.GetByID(int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()));
                            //custCntImpl.GetPerConData(int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()), dsCustCntImpl);
                            //dsCustCntImpl = custCntImpl.GetByID(int.Parse(dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString()), "",
                                                    //int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()));
                        }
                        else
                        {
                            Console.WriteLine("Excepcion de ConNum = NA");
                            throw new Exception();
                        }

                        //custCntImpl.GetByID(int.Parse(dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString()), "",
                        //                        int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()));
                        //newContact = false;
                    }catch(Exception Ex)
                    {
                        Console.WriteLine("No se encontro el contacto No. " + dsParams.Tables[0].Rows[0]["ConNum"].ToString());
                        newContact = true;
                    }

                    if(newContact)
                        custCntImpl.GetNewCustCnt(dsCustCntImpl, int.Parse(dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString()), "");

                    dsCustCntImpl.Tables["CustCnt"].Rows[0].BeginEdit();

                    foreach (DataColumn C in dsParams.Tables[0].Columns)
                    {
                        if(C.ColumnName != "CustID" && C.ColumnName != "ConNum")
                            dsCustCntImpl.Tables["CustCnt"].Rows[0][C.ColumnName] = dsParams.Tables[0].Rows[0][C.ColumnName];                          
                       
                    }

                    dsCustCntImpl.Tables["CustCnt"].Rows[0].EndEdit();
                    
                    custCntImpl.Update(dsCustCntImpl);
                    
                    //Console.WriteLine("Contact No. " + dsCustCntImpl.Tables["CustCnt"].Rows[0]["PerConID"].ToString());
                    epiResults.EpicorNum = int.Parse(dsCustCntImpl.Tables["CustCnt"].Rows[0]["PerConID"].ToString());

                }

                return epiResults;
            }catch(Exception Ex)
            {
                throw new Exception(Ex.Message);
            }
        }

        //-------------------- EPICOR CODE --------------------//
    }
}
