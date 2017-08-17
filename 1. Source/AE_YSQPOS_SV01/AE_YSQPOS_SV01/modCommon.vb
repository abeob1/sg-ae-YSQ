Imports System.Configuration
Imports System.Data.SqlClient

Module modCommon

#Region "Get Company Initialization info"
    Public Function GetCompanyInfo(ByRef oCompDef As CompanyDefault, ByRef sErrDesc As String) As Long
        Dim sFunctName As String = String.Empty
        Dim sConnection As String = String.Empty

        Try
            sFunctName = "Get Company Initialization"
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Company Initialization", sFunctName)


            oCompDef.sServer = String.Empty
            oCompDef.sLicenceServer = String.Empty
            oCompDef.sSQLServer = String.Empty
            oCompDef.sIntDBName = String.Empty
            oCompDef.sDBUser = String.Empty
            oCompDef.sDBPwd = String.Empty
            oCompDef.sRefundGLAct = String.Empty
            oCompDef.sOutLetMapping = String.Empty
            oCompDef.sRevDeptMapping = String.Empty
            oCompDef.sTippingItem = String.Empty
            oCompDef.sRoundingItem = String.Empty
            oCompDef.sExcessItem = String.Empty
            oCompDef.sServChargeItem = String.Empty

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("Server")) Then
                oCompDef.sServer = ConfigurationManager.AppSettings("Server")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("LicenceServer")) Then
                oCompDef.sLicenceServer = ConfigurationManager.AppSettings("LicenceServer")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("SQLServer")) Then
                oCompDef.sSQLServer = ConfigurationManager.AppSettings("SQLServer")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("INTDBName")) Then
                oCompDef.sIntDBName = ConfigurationManager.AppSettings("INTDBName")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("DBUser")) Then
                oCompDef.sDBUser = ConfigurationManager.AppSettings("DBUser")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("DBPwd")) Then
                oCompDef.sDBPwd = ConfigurationManager.AppSettings("DBPwd")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("RefundGLAct")) Then
                oCompDef.sRefundGLAct = ConfigurationManager.AppSettings("RefundGLAct")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("OutLetMapping")) Then
                oCompDef.sOutLetMapping = ConfigurationManager.AppSettings("OutLetMapping")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("RevDeptMapping")) Then
                oCompDef.sRevDeptMapping = ConfigurationManager.AppSettings("RevDeptMapping")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("TippingItem")) Then
                oCompDef.sTippingItem = ConfigurationManager.AppSettings("TippingItem")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("RoundingItem")) Then
                oCompDef.sRoundingItem = ConfigurationManager.AppSettings("RoundingItem")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("ExcessItem")) Then
                oCompDef.sExcessItem = ConfigurationManager.AppSettings("ExcessItem")
            End If

            If Not String.IsNullOrEmpty(ConfigurationManager.AppSettings("SrvChargeItem")) Then
                oCompDef.sServChargeItem = ConfigurationManager.AppSettings("SrvChargeItem")
            End If

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with Success", sFunctName)
            GetCompanyInfo = RTN_SUCCESS

        Catch ex As Exception
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with Error", sFunctName)
            GetCompanyInfo = RTN_ERROR
        End Try

    End Function
#End Region
#Region "Connect to Company"
    Public Function ConnectToCompany(ByRef oCompany As SAPbobsCOM.Company, ByVal sDBName As String, ByVal sDBUser As String, ByVal sPassword As String, ByRef sErrDesc As String) As Long
        ' **********************************************************************************
        '   Function    :   ConnectToCompany()
        '   Purpose     :   This function will be providing to proceed the connectivity of 
        '                   using SAP DIAPI function
        '               
        '   Parameters  :   ByRef oCompany As SAPbobsCOM.Company
        '                       oCompany =  set the SAP DI Company Object
        '                   ByRef sErrDesc AS String 
        '                       sErrDesc = Error Description to be returned to calling function
        '               
        '   Return      :   0 - FAILURE
        '                   1 - SUCCESS
        '   Author      :   SRI
        '   Date        :   October 2013
        ' **********************************************************************************

        Dim sFuncName As String = String.Empty
        Dim iRetValue As Integer = -1
        Dim iErrCode As Integer = -1
        Try
            sFuncName = "ConnectToCompany()"
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting function", sFuncName)

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Initializing the Company Object", sFuncName)
            oCompany = New SAPbobsCOM.Company

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Assigning the representing database name", sFuncName)

            oCompany.Server = p_oCompDef.sServer
            oCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_HANADB
            oCompany.CompanyDB = sDBName
            oCompany.UserName = sDBUser
            oCompany.Password = sPassword

            oCompany.LicenseServer = p_oCompDef.sLicenceServer

            oCompany.language = SAPbobsCOM.BoSuppLangs.ln_English

            oCompany.UseTrusted = False

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Connecting to the Company Database.", sFuncName)
            iRetValue = oCompany.Connect()

            If iRetValue <> 0 Then
                oCompany.GetLastError(iErrCode, sErrDesc)

                sErrDesc = String.Format("Connection to Database ({0}) {1} {2} {3}", _
                    oCompany.CompanyDB, System.Environment.NewLine, _
                                vbTab, sErrDesc)

                Throw New ArgumentException(sErrDesc)
            End If

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            ConnectToCompany = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            ConnectToCompany = RTN_ERROR
        End Try
    End Function
#End Region
#Region "Start Transaction"
    Public Function StartTransaction(ByRef sErrDesc As String) As Long
        ' ***********************************************************************************
        '   Function   :    StartTransaction()
        '   Purpose    :    Start DI Company Transaction
        '
        '   Parameters :    ByRef sErrDesc As String
        '                       sErrDesc = Error Description to be returned to calling function
        '   Return     :   0 - FAILURE
        '                   1 - SUCCESS
        '   Author     :   Jeeva
        '   Date       :   03 Aug 2015
        '   Change     :
        ' ***********************************************************************************

        Dim sFuncName As String = "StartTransaction"
        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Transaction", sFuncName)

            If p_oCompany.InTransaction Then
                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Rollback hanging transactions", sFuncName)
                p_oCompany.EndTransaction(SAPbobsCOM.BoWfTransOpt.wf_RollBack)
            End If

            p_oCompany.StartTransaction()

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Trancation Started Successfully", sFuncName)
            StartTransaction = RTN_SUCCESS

        Catch ex As Exception
            Call WriteToLogFile_Debug(ex.Message, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Error while starting Trancation", sFuncName)
            StartTransaction = RTN_ERROR
        End Try

    End Function
#End Region
#Region "Commit Transaction"
    Public Function CommitTransaction(ByRef sErrDesc As String) As Long
        ' ***********************************************************************************
        '   Function   :    CommitTransaction()
        '   Purpose    :    Commit DI Company Transaction
        '
        '   Parameters :    ByRef sErrDesc As String
        '                       sErrDesc=Error Description to be returned to calling function
        '   Return     :    0 - FAILURE
        '                   1 - SUCCESS
        '   Author     :    Jeeva
        '   Date       :    03 Aug 2015
        '   Change     :
        ' ***********************************************************************************
        Dim sFuncName As String = "CommitTransaction"
        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)
            If p_oCompany.InTransaction Then
                p_oCompany.EndTransaction(SAPbobsCOM.BoWfTransOpt.wf_Commit)
            Else
                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("No Transaction is Active", sFuncName)
            End If

            CommitTransaction = RTN_SUCCESS
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Commit Transaction Complete", sFuncName)
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Error while committing Transaciton", sFuncName)
            CommitTransaction = RTN_ERROR
        End Try
    End Function
#End Region
#Region "Rollback Transaction"
    Public Function RollbackTransaction(ByRef sErrDesc As String) As Long
        ' ***********************************************************************************
        '   Function   :    RollbackTransaction()
        '   Purpose    :    Rollback DI Company Transaction
        '
        '   Parameters :    ByRef sErrDesc As String
        '                       sErrDesc = Error Description to be returned to calling function
        '   Return     :   0 - FAILURE
        '                   1 - SUCCESS
        '   Author     :   Jeeva
        '   Date       :   31 July 2015
        '   Change     :
        ' ***********************************************************************************
        Dim sFuncName As String = String.Empty

        Try
            sFuncName = "RollbackTransaction()"
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)

            If p_oCompany.InTransaction Then
                p_oCompany.EndTransaction(SAPbobsCOM.BoWfTransOpt.wf_RollBack)
            Else
                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("No transaction is active", sFuncName)
            End If
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with Success", sFuncName)
            RollbackTransaction = RTN_SUCCESS
        Catch ex As Exception
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with Error", sFuncName)
            RollbackTransaction = RTN_ERROR
        End Try

    End Function
#End Region
#Region "Execute SQL Query"

    Public Function ExecuteNonQuery(ByVal sQuery As String, ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "ExecuteNonQuery"
        Dim sConnStr As String = "Data Source=" & p_oCompDef.sSQLServer & ";Initial Catalog=" & p_oCompDef.sIntDBName & ";User ID=" & p_oCompDef.sDBUser & "; Password=" & p_oCompDef.sDBPwd & ""
        'Dim oCon As OleDb.OleDbConnection = Nothing
        Dim oCon As SqlConnection = New SqlConnection(sConnStr)
        Dim oCmd As SqlCommand = New SqlCommand
        Dim oDa As SqlDataAdapter = New SqlDataAdapter
        Dim dtDetail As New DataTable

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)

            oCmd.CommandType = CommandType.Text
            oCmd.CommandText = sQuery
            oCmd.Connection = oCon
            If oCon.State = ConnectionState.Closed Then
                oCon.Open()
            End If

            oDa.SelectCommand = oCmd

            oDa.Fill(dtDetail)
            dtDetail.TableName = "Data"

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            ExecuteNonQuery = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            ExecuteNonQuery = RTN_ERROR
        Finally
            oCon.Dispose()
        End Try
    End Function

    Public Function ExecuteQueryReturnDataTable(ByVal sQueryString As String, ByVal sCompanyDB As String) As DataTable

        Dim sFuncName As String = "ExecuteQueryReturnDataTable"
        Dim sConstr As String = "Data Source=" & p_oCompDef.sSQLServer & ";Initial Catalog=" & sCompanyDB & ";User ID=" & p_oCompDef.sDBUser & "; Password=" & p_oCompDef.sDBPwd & ""
        'Dim sConstr As String = "DRIVER={HDBODBC32};UID=" & p_oCompDef.sDBUser & ";PWD=" & p_oCompDef.sDBPwd & ";SERVERNODE=" & p_oCompDef.sServer & ";CS=" & sCompanyDB

        Dim oCmd As New SqlCommand
        Dim oDS As DataSet = New DataSet

        Dim oCon As SqlConnection = New SqlConnection(sConstr)
        Dim dtDetail As DataTable = New DataTable
        Dim oSQLAdapter As New SqlDataAdapter

        Try

            oCmd.CommandType = CommandType.Text
            oCmd.CommandText = sQueryString
            oCmd.Connection = oCon
            If oCon.State = ConnectionState.Closed Then
                oCon.Open()
            End If

            oSQLAdapter.SelectCommand = oCmd

            oSQLAdapter.Fill(dtDetail)
            dtDetail.TableName = "Data"

        Catch ex As Exception
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("ExecuteSQL Query Error", sFuncName)
            Throw New Exception(ex.Message)
        Finally
            oCon.Dispose()
        End Try

        ExecuteQueryReturnDataTable = dtDetail

    End Function

    'Public Function ExecuteSQLQuery(ByVal sSql As String) As DataSet
    '    Dim sFuncName As String = "ExecuteSQLQuery"
    '    Dim sErrDesc As String = String.Empty

    '    Dim cmd As New Odbc.OdbcCommand
    '    Dim ods As New DataSet
    '    'Dim oSQLCommand As SqlCommand = Nothing
    '    'Dim oSQLAdapter As New SqlDataAdapter
    '    Dim oDbProviderFactoryObj As DbProviderFactory = DbProviderFactories.GetFactory("System.Data.Odbc")
    '    Dim Con As DbConnection = oDbProviderFactoryObj.CreateConnection()

    '    Try

    '        Con.ConnectionString = "DRIVER={HDBODBC32};UID=" & p_oCompDef.sDBUser & ";PWD=" & p_oCompDef.sDBPwd & ";SERVERNODE=" & p_oCompDef.sServer & ";CS=" & p_oCompDef.sSAPDBName
    '        Con.Open()

    '        cmd.CommandType = CommandType.Text
    '        cmd.CommandText = sSql
    '        cmd.Connection = Con
    '        cmd.CommandTimeout = 0
    '        Dim da As New Odbc.OdbcDataAdapter(cmd)
    '        da.Fill(ods)
    '    Catch ex As Exception
    '        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("ExecuteSQL Query Error", sFuncName)
    '        Throw New Exception(ex.Message)
    '    Finally
    '        Con.Dispose()
    '    End Try
    '    Return ods
    'End Function

    Public Function ExecuteSQLQueryDataset(ByVal sSql As String, ByVal sCompanyDB As String) As DataSet
        Dim sFuncName As String = "ExecuteSQLQueryDataset"
        Dim sErrDesc As String = String.Empty

        Dim cmd As New Odbc.OdbcCommand
        Dim ods As New DataSet
        Dim oCmd As SqlCommand = New SqlCommand()
        Dim oSQLAdapter As New SqlDataAdapter
        Dim oCon As SqlConnection

        Try

            Dim sConstr As String = "Data Source=" & p_oCompDef.sSQLServer & ";Initial Catalog=" & sCompanyDB & ";User ID=" & p_oCompDef.sDBUser & "; Password=" & p_oCompDef.sDBPwd & ""
            oCon = New SqlConnection(sConstr)
            oCmd.CommandType = CommandType.Text
            oCmd.CommandText = sSql
            oCmd.Connection = oCon
            If oCon.State = ConnectionState.Closed Then
                oCon.Open()
            End If

            oSQLAdapter.SelectCommand = oCmd

            oSQLAdapter.Fill(ods)

        Catch ex As Exception
            Call WriteToLogFile(ex.Message, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("ExecuteSQL Query Error", sFuncName)
            Throw New Exception(ex.Message)
        Finally
            oCon.Dispose()
        End Try
        Return ods
    End Function
#End Region

End Module
