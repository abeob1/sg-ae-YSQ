Module modMain

#Region "Variables"

    ' Company Default Structure
    Public Structure CompanyDefault
        Public sServer As String
        Public sLicenceServer As String
        Public sSQLServer As String
        Public sIntDBName As String
        Public sDBUser As String
        Public sDBPwd As String

        Public sOutLetMapping As String
        Public sRevDeptMapping As String
        Public sTippingItem As String
        Public sRoundingItem As String
        Public sExcessItem As String
        Public sServChargeItem As String
        
    End Structure

    'Return Value Variable Control
    Public Const RTN_SUCCESS As Int16 = 1
    Public Const RTN_ERROR As Int16 = 0
    'Debug Value Variable Control
    Public Const DEBUG_ON As Int16 = 1
    Public Const DEBUG_OFF As Int16 = 0

    ' Global variables group
    Public p_iDebugMode As Int16 = DEBUG_ON
    Public p_iErrDispMethod As Int16
    Public p_iDeleteDebugLog As Int16
    Public p_oCompDef As CompanyDefault
    Public p_oCompany As SAPbobsCOM.Company
    
#End Region

#Region "Main Method"
    Sub Main()
        Dim sFuncName As String = "Main()"
        Dim sErrDesc As String = String.Empty
        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)

            Console.Title = "POS Integration"

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling ", sFuncName)
            If GetCompanyInfo(p_oCompDef, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

            Console.WriteLine("Starting POS Integration Module")

            Start()

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
        Catch ex As Exception

        End Try
    End Sub
#End Region
   
End Module
