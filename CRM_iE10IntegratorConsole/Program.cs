using Erp.BO;
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

                    /*
                    #region E101 Customers

                    LogFile("--------------------E10 Customers OPS---------------------------");
                    LogFile("Obtenemos los Cutomers pendientes de EPICOR...");
                    VerifyEpicorChange();
                    LogFile("");

                    Thread.Sleep(2000);


                    #endregion

                    #region CRM Customers

                    LogFile("-------------------CRM Account OPS----------------------------");
                    LogFile("Obtenemos los Cutomers pendientes de CRM...");
                    VerifyCRMChange();
                    LogFile("");

                    Thread.Sleep(2000);

                    #endregion

                    #region CRM Quotes

                    LogFile("------------------CRM Quotes OPS-----------------------------");
                    LogFile("Obtenemos Quotes pendientes de CRM...");
                    CRM_QuoteChange();
                    LogFile("");
                    Thread.Sleep(2000);

                    #endregion

                    #region E101 Contacts

                    LogFile("------------------E10 Contact OPS-----------------------------");
                    LogFile("Obtenemos Contactos pendientes de Epicor...");
                    E101CreateUpdateContact();
                    LogFile("");
                    Thread.Sleep(2000);

                    #endregion

                    #region CRM Contacts                    

                    LogFile("------------------CRM Contacts OPS-----------------------------");
                    LogFile("Obtenemos Contactos pendientes de CRM...");
                    CRMCreateUpdateContact();
                    LogFile("");
                    Thread.Sleep(2000);

                    #endregion

                    #region E101 Quotes

                    LogFile("------------------E10 Quotes OPS-----------------------------");
                    LogFile("Obtenemos las cotizaciones pendientes de Epicor...");
                    E101CreateUpdCRMQuote();
                    LogFile("");
                    Thread.Sleep(5000);

                    #endregion
                    */
                    LogFile("Activating quote [QUO-34071-H7P8W9]");
                    ActivateCRMQuote("QUO-34071-H7P8W9", false);
                    LogFile("DONE! - Activating quote [QUO-34071-H7P8W9]");
                    Thread.Sleep(5000);

                }
                catch (Exception Ex)
                {
                    LogFile(Ex.Message);
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

                //LogFile("Obtenemos las Operaciones del CRM");
                DataTable dtCRMOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["crmConnection"], "Ops", 0);

                Hashtable hsCRMOps = new Hashtable();

                //LogFile("Total de Operaciones [" + dtCRMOps.Rows.Count.ToString() + "]");
                foreach (DataRow R in dtCRMOps.Rows)
                {
                    hsCRMOps.Add(R["Idx"].ToString(), R["AccountId"].ToString());
                    
                }
                #endregion

                #region Obtenemos las Operaciones que ya fueron Procesadas

                spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetCRMProcessedOps"].ToString());

                //LogFile("Obtenemos las Operaciones ya procesadas del CRM");
                DataTable dtE10ProcessedOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["iE101Connection"], "Ops", 0);

                Hashtable hsE10Ops = new Hashtable();

                //LogFile("Total de Operaciones procesadas [" + dtE10ProcessedOps.Rows.Count.ToString() + "]");
                foreach (DataRow R in dtE10ProcessedOps.Rows)
                {
                    hsE10Ops.Add(R["CRMID"].ToString(), R["CRMID"].ToString());
                    
                }

                #endregion

                #region Obtenemos las Operaciones reales a Procesar

                //LogFile("Obtenemos las Operaciones reales a Procesar...");
                Hashtable opsToProcess = CompareHashtables(hsCRMOps, hsE10Ops);

                #endregion

                LogFile("Operaciones reales a Procesar [" + opsToProcess.Count.ToString() + "]");
                foreach (DictionaryEntry entry in opsToProcess)
                {
                    LogFile("Hacemos la conexion con el CRM para procesar [" + entry.Key.ToString() + "]");
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    string acctName = string.Empty;


                    QueryExpression qry = new QueryExpression("account");
                    qry.ColumnSet = new ColumnSet(new string[] { "accountid", "accountnumber", "name"});
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = new ConditionExpression("accountid", ConditionOperator.Equal, entry.Value.ToString());
                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);                    

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    LogFile("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                    if (results != null & results.Entities.Count > 0)
                    {
                        LogFile("Account Id [" + results.Entities[0].Id.ToString() + "]");
                        acctName = results.Entities[0]["name"].ToString();
                        //Entity updAccount = new Entity("account", "accountid", Guid.Parse(results.Entities[0].Id.ToString()));

                        ColumnSet cols = new ColumnSet(new String[] { "epic6s_tempcustid", "accountnumber", "name", "address1_line1",
                            "address1_line2", "address1_line3", "address1_stateorprovince", "address1_city", "address1_postalcode",
                            "epic6s_countryid", "epic6s_customergroupid", "epic6s_termsid" });
                        Entity updAccount = clientConn.Retrieve("account", Guid.Parse(results.Entities[0].Id.ToString()), cols);

                        LogFile("--------------------------------------------------");
                        LogFile(manageNullKey(updAccount, "epic6s_countryid"));
                        //LogFile(((EntityReference)updAccount["epic6s_countryid"]).Id.ToString());
                        LogFile(manageNullKey(updAccount, "epic6s_customergroupid"));
                        //LogFile(((EntityReference)updAccount["epic6s_customergroupid"]).Id.ToString());
                        LogFile(manageNullKey(updAccount, "epic6s_termsid"));
                        //LogFile(((EntityReference)updAccount["epic6s_termsid"]).Id.ToString());
                        LogFile("--------------------------------------------------");

                        #region Obtenemos los Ids de Terms, Groups y Country

                        ColumnSet colsGroups = null;
                        Entity groupsEntity = null;
                        ColumnSet colsCountry = null;
                        Entity countryEntity = null;
                        ColumnSet colsTerms = null;
                        Entity termsEntity = null;

                        LogFile("--------------------------------------------------");
                        if (manageNullKey(updAccount, "epic6s_termsid") != "NA")
                        {
                            colsTerms = new ColumnSet(new String[] { "epic6s_terms", "epic6s_termscode" });
                            termsEntity = clientConn.Retrieve("epic6s_terms", Guid.Parse(((EntityReference)updAccount["epic6s_termsid"]).Id.ToString()), colsTerms);
                            LogFile(manageNullKey(termsEntity, "epic6s_termscode"));
                        }

                        if (manageNullKey(updAccount, "epic6s_customergroupid") != "NA")
                        {
                            colsGroups = new ColumnSet(new String[] { "epic6s_custgroupdesc", "epic6s_groupcode" });
                            groupsEntity = clientConn.Retrieve("epic6s_customergroup", Guid.Parse(((EntityReference)updAccount["epic6s_customergroupid"]).Id.ToString()), colsGroups);
                            LogFile(manageNullKey(groupsEntity, "epic6s_groupcode"));
                        }

                        if (manageNullKey(updAccount, "epic6s_countryid") != "NA")
                        {
                            colsCountry = new ColumnSet(new String[] { "epic6s_countryname", "epic6s_countrynumber" });
                            countryEntity = clientConn.Retrieve("epic6s_country", Guid.Parse(((EntityReference)updAccount["epic6s_countryid"]).Id.ToString()), colsCountry);
                            LogFile(manageNullKey(countryEntity, "epic6s_countrynumber"));
                        }
                        LogFile("--------------------------------------------------");

                        #endregion

                        #region Creamos los datos para Actualizar en Epicor.

                        LogFile("Creamos los datos para Actualizar en Epicor.");
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

                        LogFile("Mostramos los datos a actualizar...");
                       

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

                        LogFile("Ejecutamos el metodo de Epicor [AddUpdCustomer]");
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
                LogFile(epiResults.TransactionData + "[" + Ex.Message + "]");
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

                LogFile("Number of Records to Process: ["+ dtOps.Rows.Count.ToString() +"]");

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
                        LogFile("Update...");
                        acctName = results.Entities[0]["name"].ToString();
                        LogFile("Account Id a actualizar: [" + results.Entities[0].Id.ToString() + "]");
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
                        LogFile("DONE!");
                    }
                    else
                    {
                        LogFile("CREATE!");
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

                        LogFile("Mostramos los datos a actualizar...");


                        dtDetail.Rows.Add(R["CustNum"].ToString(),
                                            accId.ToString(),
                                            "1");

                        dsParams.Tables.Add(dtHeader);
                        dsParams.Tables.Add(dtDetail);

                        LogFile("Ejecutamos el metodo de Epicor [AddUpdCustomer]");
                        AddUpdCustomer(epiConnector, dsParams);

                        #endregion

                        LogFile("DONE!");

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
                LogFile(Ex.Message);
            }
        }

        public static void CRM_QuoteChange()
        {
            E101Results epiResults = new E101Results();

            try
            {
                #region Obtenemos las Operaciones del CRM

                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetQuoteOps"].ToString());

                LogFile("Obtenemos las Operaciones del CRM");
                DataTable dtCRMOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["crmConnection"], "Ops", 0);
                

                LogFile("Total de Operaciones [" + dtCRMOps.Rows.Count.ToString() + "]");

                #endregion

                #region Procesamos las Cotizaciones

                foreach (DataRow R in dtCRMOps.Rows)
                {
                    String CRMQuoteID = R[0].ToString();

                    LogFile("Procesando Quote# [" + CRMQuoteID + "]");
                    LogFile("Hacemos la conexion con el CRM para procesar [" + CRMQuoteID + "]");

                    // Hacemos la conexion con el CRM
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    QueryExpression qry = new QueryExpression("quote");
                    qry.ColumnSet = new ColumnSet(new string[] { "quotenumber", "customerid", "new_customerpo", "ownerid", "description", "new_epicorquoteid", "quoteid", "epic6s_needby" });
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = new ConditionExpression("quotenumber", ConditionOperator.Equal, CRMQuoteID);
                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);
                    

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    LogFile("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                    foreach (Entity Q in results.Entities)
                    {
                        /*LogFile("--------------------------------------------------");
                        LogFile(manageNullKey(Q, "quotenumber"));
                        LogFile(manageNullKey(Q, "customerid"));
                        LogFile(manageNullKey(Q, "new_customerpo"));
                        LogFile(manageNullKey(Q, "ownerid"));
                        LogFile(manageNullKey(Q, "description"));
                        LogFile("--------------------------------------------------");*/                       

                        ColumnSet cols = new ColumnSet(new String[] { "epic6s_tempcustid", "accountnumber", "name", "address1_line1", "address1_line2", "address1_line3", "address1_stateorprovince", "address1_city" });
                        Entity custAccount = clientConn.Retrieve("account", Guid.Parse(((EntityReference)Q["customerid"]).Id.ToString()), cols);

                        /*LogFile("--------------------------------------------------");
                        LogFile(manageNullKey(custAccount, "name"));
                        LogFile(manageNullKey(custAccount, "accountnumber"));     // CustID Epicor
                        LogFile(((EntityReference)Q["customerid"]).Id.ToString());
                        LogFile(((EntityReference)Q["ownerid"]).Id.ToString());
                        LogFile("--------------------------------------------------");*/
                        LogFile(manageNullKey(Q, "epic6s_needby"));

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

                        LogFile("Quote Details: [" + resultsDtl.Entities.Count.ToString() + "]");
                        foreach (Entity QDTL in resultsDtl.Entities)
                        {
                            LogFile(manageNullKey(QDTL, "productdescription"));

                            if (manageNullKey(QDTL, "productid") != "NA")
                            {
                                //name, productnumber, description
                                ColumnSet colsProd = new ColumnSet(new String[] { "name", "productnumber", "description" });
                                Entity prodEntity = clientConn.Retrieve("product", Guid.Parse(((EntityReference)QDTL["productid"]).Id.ToString()), colsProd);
                                LogFile("Line Guid: " + ((EntityReference)QDTL["productid"]).Id.ToString());



                                /*LogFile("--------------------------------------------------"); 
                                LogFile(manageNullKey(prodEntity, "productnumber"));
                                LogFile(manageNullKey(prodEntity, "name"));                            
                                LogFile(manageNullKey(QDTL, "quantity"));
                                LogFile(((Microsoft.Xrm.Sdk.Money)QDTL["priceperunit"]).Value.ToString());
                                LogFile("--------------------------------------------------");*/

                                dtQuoteDtl.Rows.Add(manageNullKey(prodEntity, "productnumber"),
                                                    manageNullKey(prodEntity, "name"),
                                                    manageNullKey(QDTL, "quantity"),
                                                    ((Microsoft.Xrm.Sdk.Money)QDTL["priceperunit"]).Value.ToString());
                            }else
                            {
                                dtQuoteDtl.Rows.Add(manageNullKey(QDTL, "productdescription"),
                                                    manageNullKey(QDTL, "productdescription"),
                                                    manageNullKey(QDTL, "quantity"),
                                                    ((Microsoft.Xrm.Sdk.Money)QDTL["priceperunit"]).Value.ToString());
                            }
                        }
                        //QuoteDetail - productid - description - quantity - priceperunit
                        //----------------------------------

                        dsParams.Tables.Add(dtQuoteHead);
                        dsParams.Tables.Add(dtQuoteDtl);

                        LogFile("Ejecutamos el metodo de Epicor [CreateNewQuote]");
                        epiResults = CreateNewQuote(epiConnector, dsParams);

                        LogFile("Epicor Quote Number: " + epiResults.EpicorNum.ToString());
                        LogFile("EXITO [CreateNewQuote]");

                        LogFile("Updating CRM transaction...");
                        LogFile("Epicor Id For CRM: " + manageNullKey(Q, "new_epicorquoteid")); 
                        if (manageNullKey(Q, "new_epicorquoteid") == "NA")
                        {
                            Q["new_epicorquoteid"] = epiResults.EpicorNum.ToString();
                            Q["epic6s_updatedinepicor"] = true;
                            clientConn.Update(Q);
                        }

                        LogFile("CRM Update complete.");
                        LogFile("Set CRM Op Success.");
                        String spSetCrmSuccess = File.ReadAllText(ConfigurationManager.AppSettings["spSetQuoteOPSuccess"].ToString());
                        LAVHGenericMethods.DB.EXECStoreProcedure(spSetCrmSuccess.Replace("#CRMQUOTENUM#", CRMQuoteID),
                            LAVHGenericMethods.DB.OpenConnection(ConfigurationManager.AppSettings["crmConnection"].ToString()));
                        LogFile("DONE!");

                        #endregion


                    }

                    Thread.Sleep(6000);
                }

                #endregion    

            }
            catch (Exception Ex)
            {
                LogFile(epiResults.TransactionData + "[" + Ex.Message + "]");
            }
        }

        public static void CRMCreateUpdateContact()
        {
            try
            {
                #region Obtenemos las Operaciones del CRM

                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetCRMContactOps"].ToString());

                LogFile("Obtenemos los contactos por actualizar del CRM...");
                DataTable dtCRMOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["crmConnection"], "Ops", 0);

                LogFile("Total de Operaciones [" + dtCRMOps.Rows.Count.ToString() + "]");

                foreach (DataRow R in dtCRMOps.Rows)
                {
                    String CRMContactID = R["ContactReference"].ToString();

                    LogFile("Procesando Contacto# [" + CRMContactID + "]");
                    LogFile("Hacemos la conexion con el CRM para procesar [" + CRMContactID + "]");

                    // Hacemos la conexion con el CRM
                    CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                    QueryExpression qry = new QueryExpression("contact");
                    qry.ColumnSet = new ColumnSet(new string[] { "contactid", "epic6s_epicorcontid", "telephone1", "mobilephone", "firstname", "lastname", "parentcustomerid", "emailaddress1" });
                    FilterExpression filter = new FilterExpression(LogicalOperator.And);
                    ConditionExpression con = new ConditionExpression("contactid", ConditionOperator.Equal, CRMContactID);
                    filter.Conditions.Add(con);
                    qry.Criteria.AddFilter(filter);

                    EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                    LogFile("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                    foreach (Entity Q in results.Entities)
                    {
                        ColumnSet cols = new ColumnSet(new String[] { "epic6s_tempcustid", "accountnumber", "name", "address1_line1", "address1_line2", "address1_line3", "address1_stateorprovince", "address1_city" });
                        Entity custAccount = clientConn.Retrieve("account", Guid.Parse(((EntityReference)Q["parentcustomerid"]).Id.ToString()), cols);

                        /*LogFile("--------------------------------------------------");
                        LogFile(manageNullKey(Q, "epic6s_epicorcontid"));
                        LogFile(manageNullKey(Q, "telephone1"));
                        LogFile(manageNullKey(Q, "mobilephone"));
                        LogFile(manageNullKey(Q, "firstname"));
                        LogFile(manageNullKey(Q, "lastname"));
                        LogFile(manageNullKey(custAccount, "accountnumber"));
                        LogFile(manageNullKey(Q, "emailaddress1"));
                        LogFile("--------------------------------------------------");*/

                        #region Creamos los datos para Actualizar en Epicor.

                        LogFile("Creamos los datos para Actualizar en Epicor.");
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

                        LogFile("Ejecutamos el metodo de Epicor [AddUpdtContact]");
                        try
                        {
                            AddUpdtContact(epiConnector, dsParams);
                            LogFile("DONE!");
                            if (manageNullKey(Q, "epic6s_epicorcontid") == "NA")
                            {
                                LogFile("Enviamos No. de contacto a CRM...");
                                E101CreateUpdateContact();
                            }

                            LogFile("Set CRM Op Success.");
                            String spSetCrmSuccess = File.ReadAllText(ConfigurationManager.AppSettings["spSetContactOPSuccess"].ToString());
                            LAVHGenericMethods.DB.EXECStoreProcedure(spSetCrmSuccess.Replace("#IDX#", R["Idx"].ToString()),
                                LAVHGenericMethods.DB.OpenConnection(ConfigurationManager.AppSettings["crmConnection"].ToString()));
                            LogFile("DONE!");
                            //spSetContactOPSuccess
                        }
                        catch(Exception Ex)
                        {
                            LogFile(Ex.Message);
                        }

                        #endregion

                    }
                }

                    #endregion
            }
            catch (Exception Ex)
            {
                LogFile(Ex.Message);
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
                LogFile(Ex.Message);
            }
        }

        public static void DeleteQuoteLine(string CRMQuoteNum)
        {
            try
            {
                CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());
                                
                QueryExpression qry = new QueryExpression("quote");
                qry.ColumnSet = new ColumnSet(new string[] { "quoteid", "quotenumber", "customerid", "new_customerpo", "ownerid", "description", "new_epicorquoteid", "quoteid", "epic6s_needby" });
                FilterExpression filter = new FilterExpression(LogicalOperator.And);
                ConditionExpression con = new ConditionExpression("quotenumber", ConditionOperator.Equal, CRMQuoteNum);
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

                    LogFile("Quote Details: [" + resultsDtl.Entities.Count.ToString() + "]");
                    foreach (Entity QDTL in resultsDtl.Entities)
                    {
                        //name, productnumber, description
                        //ColumnSet colsProd = new ColumnSet(new String[] { "quotedetailid", "name", "productnumber", "description" });
                        //Entity prodEntity = clientConn.Retrieve("product", Guid.Parse(((EntityReference)QDTL["productid"]).Id.ToString()), colsProd);
                        LogFile("Obtenemos los datos del Produto..");

                        LogFile("Line Guid to DEL: " + QDTL["quotedetailid"].ToString());

                        clientConn.Delete("quotedetail", Guid.Parse(QDTL["quotedetailid"].ToString()));
                        
                        LogFile("DEL SUCCESS");
                    }
                    clientConn.Update(Q);
                }
                //b147d7ee-e86e-e411-80d3-005056be1c27
                //b147d7ee-e86e-e411-80d3-005056be1c27
            }
            catch (Exception Ex)
            {
                LogFile(Ex.Message);
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

                LogFile("Number of Records to Process: [" + dtOps.Rows.Count.ToString() + "]");

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
                        LogFile("Update...");                        
                        LogFile("Contact Id a actualizar: [" + results.Entities[0].Id.ToString() + "]");
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
                        LogFile("DONE!");
                    }
                    else
                    {
                        LogFile("Creating Contact!");
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
                        LogFile("Contact ID: " + accId.ToString());
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
                LogFile(Ex.Message);
            }
        }

        public static void E101CreateUpdCRMQuote()
        {            
            
            try
            {

                String spGetOps = File.ReadAllText(ConfigurationManager.AppSettings["spGetE101QuoteOps"].ToString());
                String spUpdOps = String.Empty;

                DataTable dtOps = LAVHGenericMethods.DB.GetDataTableFromDB(spGetOps,
                                                                            ConfigurationManager.AppSettings["iE101Connection"], "Ops", 0);

                CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                LogFile("Number of Records to Process: [" + dtOps.Rows.Count.ToString() + "]");

                QuoteImpl quoteImpl = null;
                QuoteDataSet dsQuoteImpl = null;

                foreach (DataRow R in dtOps.Rows)
                {
                    using (Session session = new Session(epiConnector.epiUser, epiConnector.epiPassword, epiConnector.epiServer, Session.LicenseType.Default, epiConnector.epiConfig))
                    {

                        LogFile("Exito al hacer la conexion");
                        session.CompanyID = R["Company"].ToString();

                        quoteImpl = Ice.Lib.Framework.WCFServiceSupport.CreateImpl<QuoteImpl>(session, Epicor.ServiceModel.Channels.ImplBase<QuoteSvcContract>.UriPath);

                        dsQuoteImpl = new QuoteDataSet();

                        #region Verificamos si existe el Quote en Epicor

                        try
                        {
                            LogFile("Looking for Quote...");
                            dsQuoteImpl = quoteImpl.GetByID(int.Parse(R["QuoteNum"].ToString()));
                            LogFile("Quote already exists, Updating...");

                        }
                        catch (Exception Ex)
                        {
                            throw new Exception("Quote Not Found...[" + Ex.Message + "]");
                        }

                        #endregion


                        if (R["CRMQuoteId"].ToString() != "" || dsQuoteImpl.QuoteHed[0]["QuoteComment"].ToString() != "")
                        {
                            #region Verificamos si Existe el Quote en CRM

                            LogFile("Looking for CRM Quote [" + R["CRMQuoteId"].ToString() + "]");

                            QueryExpression qry = new QueryExpression("quote");
                            qry.ColumnSet = new ColumnSet(new string[] { "quoteid", "quotenumber", "customerid", "new_customerpo", "ownerid", "description", "new_epicorquoteid", "quoteid", "epic6s_needby" });
                            FilterExpression filter = new FilterExpression(LogicalOperator.And);
                            ConditionExpression con = new ConditionExpression("quotenumber", ConditionOperator.Equal, R["CRMQuoteId"].ToString());
                            filter.Conditions.Add(con);
                            qry.Criteria.AddFilter(filter);


                            EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                            LogFile("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                            foreach (Entity Q in results.Entities)
                            { 
                                LogFile("Deleting Quote Details...");
                                DeleteQuoteLine(R["CRMQuoteId"].ToString());

                                //create a batch for commit/rollback scenario
                                LogFile("Creamos Batch...");
                                Guid batchID = clientConn.CreateBatchOperationRequest("quotebatch", true, false);

                                Dictionary<string, CrmDataTypeWrapper> quoteArray = new Dictionary<string, CrmDataTypeWrapper>();
                                Dictionary<string, CrmDataTypeWrapper> detailArray = new Dictionary<string, CrmDataTypeWrapper>();

                                //set write-in
                                CrmDataTypeWrapper quote = new CrmDataTypeWrapper();
                                quote.ReferencedEntity = "quote";
                                quote.Value = Guid.Parse(Q["quoteid"].ToString()); //new Guid("9222A75A-743D-E711-80E4-3863BB34E918");
                                quote.Type = CrmFieldType.Lookup;

                                quoteArray["epic6s_updatedinepicor"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                detailArray["quoteid"] = quote;

                                foreach (DataRow quoteDtlRow in dsQuoteImpl.QuoteDtl)
                                {
                                    //set write-in
                                    //detailArray["isproductoverriden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                    detailArray["isproductoverridden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                    //set price overridden
                                    //detailArray["ispriceoverriden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                    detailArray["ispriceoverridden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                    //set the description
                                    detailArray["productdescription"] = new CrmDataTypeWrapper(quoteDtlRow["PartNum"].ToString(), CrmFieldType.String);

                                    //set the price
                                    detailArray["priceperunit"] = new CrmDataTypeWrapper((decimal)(quoteDtlRow["DocExpUnitPrice"]), CrmFieldType.CrmMoney);

                                    //qty
                                    detailArray["quantity"] = new CrmDataTypeWrapper((decimal)(quoteDtlRow["OrderQty"]), CrmFieldType.CrmDecimal);

                                    //Updated by service.
                                    detailArray["epic6s_updatedinepicor"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean); 

                                    LogFile("Creamos Detalle...");
                                    clientConn.CreateNewRecord("quotedetail", detailArray, batchId: batchID);
                                }
                                //execute batch
                                ExecuteMultipleResponse response = clientConn.ExecuteBatch(batchID);
                                LogFile("Exito!");

                            }

                            #endregion
                        }
                        else
                        {

                            //create a batch for commit/rollback scenario
                            LogFile("Creamos Batch...");
                            Guid batchID = clientConn.CreateBatchOperationRequest("quotebatch", true, false);

                            //create quote for this account
                            //Entity quote = new Entity("quote");

                            Dictionary<string, CrmDataTypeWrapper> quoteArray = new Dictionary<string, CrmDataTypeWrapper>();


                            LogFile("Obtenemos Id Cliente...");
                            CrmDataTypeWrapper cust = new CrmDataTypeWrapper();
                            cust.ReferencedEntity = "account";
                            cust.Value = Guid.Parse(R["CRMCustID"].ToString());
                            cust.Type = CrmFieldType.Customer;

                            quoteArray["customerid"] = cust;

                            //Set the Name of the Quote
                            quoteArray["name"] = new CrmDataTypeWrapper("Epicor Quote Num: " + R["QuoteNum"].ToString(), CrmFieldType.String);
                            quoteArray["new_epicorquoteid"] = new CrmDataTypeWrapper(R["QuoteNum"].ToString(), CrmFieldType.String);
                            quoteArray["epic6s_updatedinepicor"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);
                            //quoteArray["epic6s_tasksetep"] = new CrmDataTypeWrapper(dsQuoteImpl.QuoteHed.Rows[0]["TaskSetID"].ToString(), CrmFieldType.String); 

                            //set the name
                            Guid quoteID = Guid.NewGuid();
                            quoteArray["quoteid"] = new CrmDataTypeWrapper(quoteID, CrmFieldType.UniqueIdentifier);

                            //Obtenemos la lista de precios a utilizar
                            CrmDataTypeWrapper pricelevel = new CrmDataTypeWrapper();
                            pricelevel.ReferencedEntity = "pricelevel";
                            pricelevel.Value = new Guid("67C3C505-CD5A-E411-80D2-005056BE1C27");
                            pricelevel.Type = CrmFieldType.Lookup;
                            quoteArray["pricelevelid"] = pricelevel; //new CrmDataTypeWrapper(new EntityReference("pricelevel", new Guid("9222A75A-743D-E711-80E4-3863BB34E918")), CrmFieldType.Lookup);


                            LogFile("Creamos Encabezado...");
                            clientConn.CreateNewRecord("quote", quoteArray, batchId: batchID);

                            LogFile("Cargamos datos de los detalles...");
                            Dictionary<string, CrmDataTypeWrapper> detailArray = new Dictionary<string, CrmDataTypeWrapper>();

                            //set write-in
                            CrmDataTypeWrapper quote = new CrmDataTypeWrapper();
                            quote.ReferencedEntity = "quote";
                            quote.Value = quoteID; //new Guid("9222A75A-743D-E711-80E4-3863BB34E918");
                            quote.Type = CrmFieldType.Lookup;

                            detailArray["quoteid"] = quote;

                            foreach (DataRow quoteDtlRow in dsQuoteImpl.QuoteDtl)
                            {
                                //set write-in
                                //detailArray["isproductoverriden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                detailArray["isproductoverridden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                //set price overridden
                                //detailArray["ispriceoverriden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                detailArray["ispriceoverridden"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                //set the description
                                detailArray["productdescription"] = new CrmDataTypeWrapper(quoteDtlRow["PartNum"].ToString(), CrmFieldType.String);

                                //set the price
                                detailArray["priceperunit"] = new CrmDataTypeWrapper((decimal)(quoteDtlRow["DocExpUnitPrice"]), CrmFieldType.CrmMoney);

                                //qty
                                detailArray["quantity"] = new CrmDataTypeWrapper((decimal)(quoteDtlRow["OrderQty"]), CrmFieldType.CrmDecimal);

                                //Updated in Epicor
                                detailArray["epic6s_updatedinepicor"] = new CrmDataTypeWrapper(true, CrmFieldType.CrmBoolean);

                                LogFile("Creamos Detalle...");
                                clientConn.CreateNewRecord("quotedetail", detailArray, batchId: batchID);
                            }
                            //execute batch
                            ExecuteMultipleResponse response = clientConn.ExecuteBatch(batchID);
                            LogFile("Exito!");

                            if (dsQuoteImpl.Tables["QuoteHed"].Rows[0]["QuoteComment"].ToString() == "")
                            {
                                ColumnSet cols = new ColumnSet(new String[] { "quotenumber" });
                                Entity quoteData = clientConn.Retrieve("quote", quoteID, cols);

                                LogFile("Created Quote number: " + quoteData["quotenumber"].ToString());

                                dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                                dsQuoteImpl.Tables["QuoteHed"].Rows[0]["QuoteComment"] = quoteData["quotenumber"].ToString();
                                dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                                dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                                LogFile("Updating...");
                                LogFile("UPDATE SERVCE VALUE: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["LeadRating"].ToString() + "]");
                                quoteImpl.Update(dsQuoteImpl);
                                LogFile("Success");
                            }
                        }

                    }
                    Hashtable hsUD01 = new Hashtable();
                    hsUD01.Add("Key1", R["Idx"].ToString());
                    hsUD01.Add("Key2", R["QuoteNum"].ToString());
                    hsUD01.Add("Key3", "E101QuoteData");
                    hsUD01.Add("Key4", "");
                    hsUD01.Add("Key5", "");
                    hsUD01.Add("CheckBox01", "true");

                    insertDataUD01(epiConnector, hsUD01);
                }
            }
            catch (Exception Ex)
            {
                LogFile(Ex.Message);
            }
            
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
                        LogFile("Buscamos: [" + custID + "]");
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

                                LogFile("tmpCustID = NA so: " + tmpCustID);
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
                                //LogFile("CustID = NA so: " + dsCustImpl.Tables["Customer"].Rows[0]["CustID"].ToString());
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

                LogFile("Hacemos Conexion con Epicor...");

                using (Session session = new Session(conf.epiUser, conf.epiPassword, conf.epiServer, Session.LicenseType.Default, conf.epiConfig))
                {
                    LogFile("Exito al hacer la conexion");
                    session.CompanyID = dsParams.Tables[0].Rows[0]["Company"].ToString();
                    //session.PlantID = dsParams.Tables["RMAHead"].Rows[0]["Plant"].ToString();

                    QuoteImpl quoteImpl =
                        Ice.Lib.Framework.WCFServiceSupport.CreateImpl<QuoteImpl>(session, Epicor.ServiceModel.Channels.ImplBase<QuoteSvcContract>.UriPath);

                    QuoteDataSet dsQuoteImpl = new QuoteDataSet();

                    #region Verificamos si existe el Quote

                    try
                    {
                        LogFile("Looking for Quote...");
                        int quoteNum = (dsParams.Tables[0].Rows[0]["QuoteNum"].ToString() != "NA") ? int.Parse(dsParams.Tables[0].Rows[0]["QuoteNum"].ToString()) : 0;
                        dsQuoteImpl = quoteImpl.GetByID(quoteNum);
                        LogFile("Quote already exists, Updating...");
                        newQuote = false;
                    }
                    catch (Exception Ex)
                    {
                        LogFile("Quote Not Found...");
                        newQuote = true;
                    }
                    #endregion

                    #region Obtenemos el Encabezado

                    // Creamos las bases para la Cotizacion
                    if (newQuote)
                    {
                        LogFile("Creating Quote Header...");
                        quoteImpl.GetNewQuoteHed(dsQuoteImpl);
                    }
                    LogFile("CustID: " + dsParams.Tables[0].Rows[0]["CustID"].ToString());

                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["CustomerCustID"] = dsParams.Tables[0].Rows[0]["CustID"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                    LogFile("XXX - GetCustomerInfo: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
                    quoteImpl.GetCustomerInfo(dsQuoteImpl);

                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["CustomerCustID"] = dsParams.Tables[0].Rows[0]["CustID"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["QuoteComment"] = dsParams.Tables[0].Rows[0]["QuoteComment"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["PONum"] = dsParams.Tables[0].Rows[0]["PONum"].ToString();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                    //dsQuoteImpl.Tables["QuoteHed"].Rows[0]["NeedByDate"] = dsParams.Tables[0].Rows[0]["NeedByDate"].ToString(); 
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                    LogFile("Updating...");
                    LogFile("XXX - GetCustomerInfo - Update: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
                    quoteImpl.Update(dsQuoteImpl);
                    LogFile("Success");

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

                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                    dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                    LogFile("XXX - After Delete Lines - Update: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
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

                    LogFile("Lineas a Crear: [" + dsParams.Tables[1].Rows.Count + "]");
                    foreach (DataRow quoteLine in dsParams.Tables[1].Rows)
                    {
                        partNum = quoteLine["PartNum"].ToString();

                        quoteImpl.GetNewQuoteDtl(dsQuoteImpl, _QuoteNum);

                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                        LogFile("XXX - ChangePartNumMaster: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
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

                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                        LogFile("XXX - ChangePartNum: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
                        quoteImpl.ChangePartNum(dsQuoteImpl, lSubstitutePartsExist, "");

                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                        LogFile("XXX - ChangePartNum - Update: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
                        quoteImpl.Update(dsQuoteImpl);

                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                        LogFile("XXX - GetDtlUnitPriceInfo_User: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
                        quoteImpl.GetDtlUnitPriceInfo_User(true, true, false, true, dsQuoteImpl);

                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].BeginEdit();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["PartNum"] = quoteLine["PartNum"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["LineDesc"] = quoteLine["LineDesc"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["OrderQty"] = quoteLine["OrderQty"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex]["DocExpUnitPrice"] = quoteLine["DocExpUnitPrice"].ToString();
                        dsQuoteImpl.Tables["QuoteDtl"].Rows[rowIndex].EndEdit();

                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].BeginEdit();
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"] = "CRM";
                        dsQuoteImpl.Tables["QuoteHed"].Rows[0].EndEdit();

                        LogFile("XXX - GetDtlUnitPriceInfo_User - Update: [" + dsQuoteImpl.Tables["QuoteHed"].Rows[0]["Reference"].ToString() + "]");
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

                LogFile("Hacemos Conexion con Epicor...");

                using (Session session = new Session(conf.epiUser, conf.epiPassword, conf.epiServer, Session.LicenseType.Default, conf.epiConfig))
                {
                    LogFile("Exito al hacer la conexion");
                    session.CompanyID = dsParams.Tables[0].Rows[0]["Company"].ToString();
                    //session.PlantID = dsParams.Tables["RMAHead"].Rows[0]["Plant"].ToString();                    

                    CustomerImpl custImpl =
                        Ice.Lib.Framework.WCFServiceSupport.CreateImpl<CustomerImpl>(session, Epicor.ServiceModel.Channels.ImplBase<CustomerSvcContract>.UriPath);

                    CustomerDataSet dsCustImpl = new CustomerDataSet();

                    try
                    {                        
                        LogFile("Buscamos: [" + dsParams.Tables[0].Rows[0]["custID"].ToString() + "]"); 
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
                        LogFile("Buscamos el contacto No. " + dsParams.Tables[0].Rows[0]["ConNum"].ToString() +
                            " del Cliente No. " + dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString());

                        if (dsParams.Tables[0].Rows[0]["ConNum"].ToString() != "NA")
                        {
                            bool morePages = false;
                            dsCustCntImpl = custCntImpl.GetRows("CustNum = '" + dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString() + "' BY Name", "", 0, 0, out morePages);
                            var rowCount = (from result1 in dsCustCntImpl.Tables["CustCnt"].AsEnumerable()
                                            where result1.Field<int>("PerConID") == int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()) 
                                            select result1).Count();

                            LogFile("Row Count: " + rowCount.ToString());
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
                            LogFile("Excepcion de ConNum = NA");
                            throw new Exception();
                        }

                        //custCntImpl.GetByID(int.Parse(dsCustImpl.Tables["Customer"].Rows[0]["CustNum"].ToString()), "",
                        //                        int.Parse(dsParams.Tables[0].Rows[0]["ConNum"].ToString()));
                        //newContact = false;
                    }catch(Exception Ex)
                    {
                        LogFile("No se encontro el contacto No. " + dsParams.Tables[0].Rows[0]["ConNum"].ToString());
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
                    
                    //LogFile("Contact No. " + dsCustCntImpl.Tables["CustCnt"].Rows[0]["PerConID"].ToString());
                    epiResults.EpicorNum = int.Parse(dsCustCntImpl.Tables["CustCnt"].Rows[0]["PerConID"].ToString());

                }

                return epiResults;
            }catch(Exception Ex)
            {
                throw new Exception(Ex.Message);
            }
        }

        //-------------------- EPICOR CODE --------------------//

        public static void LogFile(string message)
        {
            try
            {
                Console.WriteLine(message);
                LAVHGenericMethods.LogHandler.LogMessageHandler(LAVHGenericMethods.LogHandler.MessageType.INFO, "CRM - E101", message, ConfigurationManager.AppSettings["pathLog"].ToString());
            }
            catch (Exception Ex)
            {
                throw new Exception(Ex.Message);
            }
        }

        public static void ActivateCRMQuote(String crmQuoteNum, bool ActivateDeactivate)
        {
            try
            {
                CrmServiceClient clientConn = new CrmServiceClient(ConfigurationManager.AppSettings["crmConnStringDEV"].ToString());

                QueryExpression qry = new QueryExpression("quote");
                qry.ColumnSet = new ColumnSet(new string[] { "quoteid", "quotenumber", "customerid", "new_customerpo", "ownerid", "description", "new_epicorquoteid", "quoteid", "epic6s_needby" });
                FilterExpression filter = new FilterExpression(LogicalOperator.And);
                ConditionExpression con = new ConditionExpression("quotenumber", ConditionOperator.Equal, crmQuoteNum);
                filter.Conditions.Add(con);
                qry.Criteria.AddFilter(filter);


                EntityCollection results = clientConn.RetrieveMultiple(qry);  //adjust the CrmServiceClient client object to the name used in your code

                LogFile("Registros encontrados: [" + results.Entities.Count.ToString() + "]");
                foreach (Entity Q in results.Entities)
                {
                    // Change the Quote to Active State.
                    SetStateRequest activateQuote = new SetStateRequest();
                    activateQuote.EntityMoniker = new EntityReference("quote", new Guid(Q["quoteid"].ToString()));
                    if (ActivateDeactivate)
                    {
                        activateQuote.State = new OptionSetValue(1); // Active
                        activateQuote.Status = new OptionSetValue(2); // InProgress

                    }else
                    {
                        activateQuote.State = new OptionSetValue(0); // Active
                        activateQuote.Status = new OptionSetValue(1); // InProgress
                    }
                    clientConn.Execute(activateQuote);
                }

                

            }
            catch (Exception Ex)
            {
                LogFile(Ex.Message);
            }
        }

        //Deactivate a record
        public static void DeactivateRecord(string entityName, Guid recordId, IOrganizationService organizationService)
        {
            var cols = new ColumnSet(new[] { "statecode", "statuscode" });

            //Check if it is Active or not
            var entity = organizationService.Retrieve(entityName, recordId, cols);

            if (entity != null && entity.GetAttributeValue<OptionSetValue>("statecode").Value == 0)
            {
                //StateCode = 1 and StatusCode = 2 for deactivating Account or Contact
                SetStateRequest setStateRequest = new SetStateRequest()
                {
                    EntityMoniker = new EntityReference
                    {
                        Id = recordId,
                        LogicalName = entityName,
                    },
                    State = new OptionSetValue(1),
                    Status = new OptionSetValue(2)
                };
                organizationService.Execute(setStateRequest);
            }
        }

        //Activate a record
        public static void ActivateRecord(string entityName, Guid recordId, IOrganizationService organizationService)
        {
            var cols = new ColumnSet(new[] { "statecode", "statuscode" });

            //Check if it is Inactive or not
            var entity = organizationService.Retrieve(entityName, recordId, cols);

            if (entity != null && entity.GetAttributeValue<OptionSetValue>("statecode").Value == 1)
            {
                //StateCode = 0 and StatusCode = 1 for activating Account or Contact
                SetStateRequest setStateRequest = new SetStateRequest()
                {
                    EntityMoniker = new EntityReference
                    {
                        Id = recordId,
                        LogicalName = entityName,
                    },
                    State = new OptionSetValue(0),
                    Status = new OptionSetValue(1)
                };
                organizationService.Execute(setStateRequest);
            }
        }
    }
}
