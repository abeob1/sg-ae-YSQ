Module modProcess

    Private dtSalesTransDet As New DataTable
    Private dtCollectionDet As New DataTable

    Public Sub Start()
        Dim sFuncName As String = "Start()"
        Dim sErrDesc As String = String.Empty
        Dim sSQL As String = String.Empty
        Dim bIsTranscStart As Boolean = False

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting function", sFuncName)

            Console.WriteLine("Processing Invoice")
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling ProcessInvoiceDetails()", sFuncName)
            If ProcessInvoiceDetails(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
            Console.WriteLine("Invoice Processing completed")

            Console.WriteLine("Processing collections")
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling ProcessCollectionDetails()", sFuncName)
            If ProcessCollectionDetails(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
            Console.WriteLine("Collection processing completed")

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
        Catch ex As Exception
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            End
        End Try
    End Sub

    Private Function ProcessInvoiceDetails(ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "ProcessInvoiceDetails"
        Dim sSql As String = String.Empty

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function ", sFuncName)

            sSql = "SELECT A.FileID,A.POSNo,A.POSOutlet,A.DocDate,ISNULL(A.TotalGrossAmt,0) [TotalGrossAmt],ISNULL(A.SvcCharge,0) [SvcCharge], " & _
                  " ISNULL(A.GST,0) [GST],ISNULL(A.Rounding,0) [Rounding],ISNULL(A.Excess,0) [Excess],ISNULL(A.Tips,0) [Tips], B.POSItemCode,B.POSDept, " & _
                  " B.DiscCode,B.DiscItem,B.SetMealCode,ISNULL(B.Price,0) [Price],ISNULL(B.Qty,0) [Qty],ISNULL(B.SubTotal,0) [SubTotal],A.Covers,ISNULL(B.DiscAmt,0) [DiscAmt],B.Adjustment " & _
                  " FROM " & p_oCompDef.sIntDBName & ".dbo.SalesTransHeader A INNER JOIN " & p_oCompDef.sIntDBName & ".dbo.SalesTransDetails B ON B.FileID = A.FileID " & _
                  " WHERE ISNULL(A.ARDocEntry,'') = '' AND ISNULL(A.Status,'FAIL') = 'FAIL' AND A.RUpdated = 1 "
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSql, sFuncName)
            dtSalesTransDet = ExecuteQueryReturnDataTable(sSql, p_oCompDef.sIntDBName)

            Dim oDataView As DataView = New DataView(dtSalesTransDet)
            If oDataView.Count > 0 Then
                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Grouping datas based on outlet code", sFuncName)
                Console.WriteLine("Grouping datas based on outlet code")

                Dim oDtGroup As DataTable = oDataView.Table.DefaultView.ToTable(True, "POSOutlet")
                For i As Integer = 0 To oDtGroup.Rows.Count - 1
                    If Not (oDtGroup.Rows(i).Item(0).ToString.Trim() = String.Empty Or oDtGroup.Rows(i).Item(0).ToString.ToUpper().Trim() = "POSOUTLET") Then

                        Dim sOutlet As String = String.Empty
                        sOutlet = oDtGroup.Rows(i).Item(0).ToString.Trim()

                        Console.WriteLine("Outlet code is " & sOutlet)

                        oDataView.RowFilter = "POSOutlet = '" & oDtGroup.Rows(i).Item(0).ToString.Trim() & "' "

                        If oDataView.Count > 0 Then
                            Dim odtTable As New DataTable
                            odtTable = oDataView.ToTable()
                            Dim oDvDataview As DataView = New DataView(odtTable)

                            If oDvDataview.Count > 0 Then
                                Dim sSAPDBName As String = String.Empty
                                Dim sSAPUser As String = String.Empty
                                Dim sSAPPass As String = String.Empty
                                Dim oDs As New DataSet

                                sSql = "SELECT A.Entity,B.SAPUserName,B.SAPPassWord,A.CardCode FROM SAP_POS_OUTLET A INNER JOIN AE_COMPANYDATA B ON B.Entity = A.Entity WHERE A.POSOutletCode = '" & sOutlet & "' "
                                oDs = ExecuteSQLQueryDataset(sSql, p_oCompDef.sIntDBName)
                                If oDs.Tables(0).Rows.Count > 0 Then
                                    sSAPDBName = oDs.Tables(0).Rows(0)("Entity").ToString()
                                    sSAPUser = oDs.Tables(0).Rows(0)("SAPUserName").ToString()
                                    sSAPPass = oDs.Tables(0).Rows(0)("SAPPassWord").ToString()

                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling ConnectToCompany()", sErrDesc)
                                    If ConnectToCompany(p_oCompany, sSAPDBName, sSAPUser, sSAPPass, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                    If p_oCompany.Connected Then
                                        Console.WriteLine("Connected to company " & p_oCompany.CompanyDB)

                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling StartTransaction() ", sFuncName)
                                        If StartTransaction(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling CreateARInvoice()", sFuncName)
                                        If CreateARInvoice(oDvDataview, sErrDesc) <> RTN_SUCCESS Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling RollbackTransaction()", sFuncName)
                                            If RollbackTransaction(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                        Else
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling CommitTransaction()", sFuncName)
                                            If CommitTransaction(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                        End If

                                    End If
                                Else
                                    Throw New ArgumentException("Company Information not found for the outlet code " & sOutlet)
                                End If
                            End If

                        End If
                    End If
                Next

                If p_oCompany.Connected Then
                    p_oCompany.Disconnect()
                End If

            End If

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            ProcessInvoiceDetails = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            ProcessInvoiceDetails = RTN_ERROR
        End Try
    End Function

    Private Function ProcessCollectionDetails(ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "ProcessCollectionDetails"
        Dim sSql As String = String.Empty

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)

            sSql = "SELECT DISTINCT A.POSOutlet " & _
                   " FROM " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails A " & _
                   " INNER JOIN " & p_oCompDef.sIntDBName & ".dbo.SalesTransHeader B ON B.FileID = A.FileID AND B.POSOutlet = A.POSOutlet AND B.POSNo = A.POSNo " & _
                   " WHERE ISNULL(A.RCDocEntry,'') = '' AND ISNULL(B.ARDocEntry,'') <> '' AND A.RUpdated = 1 "
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing Query " & sSql, sFuncName)
            dtCollectionDet = ExecuteQueryReturnDataTable(sSql, p_oCompDef.sIntDBName)

            Dim oDvCollections As DataView = New DataView(dtCollectionDet)
            If oDvCollections.Count > 0 Then
                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Grouping datas based on outlet code", sFuncName)

                Dim oDtGroup As DataTable = oDvCollections.Table.DefaultView.ToTable(True, "POSOutlet")
                For i As Integer = 0 To oDtGroup.Rows.Count - 1
                    If Not (oDtGroup.Rows(i).Item(0).ToString.Trim() = String.Empty Or oDtGroup.Rows(i).Item(0).ToString.ToUpper().Trim() = "POSOUTLET") Then

                        Dim sOutlet As String = String.Empty
                        sOutlet = oDtGroup.Rows(i).Item(0).ToString.Trim()

                        Dim sSAPDBName As String = String.Empty
                        Dim sSAPUser As String = String.Empty
                        Dim sSAPPass As String = String.Empty
                        Dim oDs As New DataSet

                        sSql = "SELECT A.Entity,B.SAPUserName,B.SAPPassWord FROM SAP_POS_OUTLET A INNER JOIN AE_COMPANYDATA B ON B.Entity = A.Entity WHERE A.POSOutletCode = '" & sOutlet & "' "
                        oDs = ExecuteSQLQueryDataset(sSql, p_oCompDef.sIntDBName)
                        If oDs.Tables(0).Rows.Count > 0 Then
                            sSAPDBName = oDs.Tables(0).Rows(0)("Entity").ToString()
                            sSAPUser = oDs.Tables(0).Rows(0)("SAPUserName").ToString()
                            sSAPPass = oDs.Tables(0).Rows(0)("SAPPassWord").ToString()

                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling ConnectToCompany()", sErrDesc)
                            If ConnectToCompany(p_oCompany, sSAPDBName, sSAPUser, sSAPPass, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                            If p_oCompany.Connected Then
                                Console.WriteLine("Connected to company " & p_oCompany.CompanyDB)

                                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling StartTransaction() ", sFuncName)
                                If StartTransaction(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling CreateCollectionEntry()", sFuncName)
                                If CreateCollectionEntry(sOutlet, sErrDesc) <> RTN_SUCCESS Then
                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling RollbackTransaction()", sFuncName)
                                    If RollbackTransaction(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                Else
                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Calling CommitTransaction()", sFuncName)
                                    If CommitTransaction(sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                End If

                            End If
                        Else
                            Throw New ArgumentException("Company Information not found for the outlet code " & sOutlet)
                        End If

                    End If
                Next

                If p_oCompany.Connected Then
                    p_oCompany.Disconnect()
                End If

            End If

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            ProcessCollectionDetails = RTN_SUCCESS

        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            ProcessCollectionDetails = RTN_ERROR
        End Try
    End Function

    Private Function CreateARInvoice(ByVal oDv As DataView, ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "CreateARInvoice"
        Dim sSQL As String = String.Empty
        Dim sFileId As String = String.Empty
        Dim sCardCode As String = String.Empty
        Dim sPOSNo As String = String.Empty
        Dim sPOSDept As String = String.Empty
        Dim sOutlet As String = String.Empty
        Dim sBranch As String = String.Empty
        Dim sOcrCode As String = String.Empty
        Dim sOcrCode2 As String = String.Empty
        Dim sWhseCode As String = String.Empty
        Dim sWhseBin As String = String.Empty
        Dim sDocDate As String = String.Empty
        Dim sDiscCode As String = String.Empty
        Dim sBinLoc As String = String.Empty
        Dim oDs As New DataSet
        Dim oRs As SAPbobsCOM.Recordset
        oRs = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.BoRecordset)

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)

            Dim oDtGroup As DataTable = oDv.Table.DefaultView.ToTable(True, "FileID")
            For i As Integer = 0 To oDtGroup.Rows.Count - 1
                If Not (oDtGroup.Rows(i).Item(0).ToString.Trim() = String.Empty Or oDtGroup.Rows(i).Item(0).ToString.ToUpper().Trim() = "FILEID") Then
                    oDv.RowFilter = "FileID = '" & oDtGroup.Rows(i).Item(0).ToString.Trim() & "' "
                    sFileId = oDtGroup.Rows(i).Item(0).ToString.Trim()
                    If oDv.Count > 0 Then
                        Dim oDt As New DataTable
                        oDt = oDv.ToTable()
                        Dim oInvDv As DataView = New DataView(oDt)

                        sOutlet = oDv(0)(2).ToString().Trim()

                        Console.WriteLine("Processing File id " & sFileId & " for outlet " & sOutlet)

                        If oInvDv.Count > 0 Then
                            If p_oCompDef.sOutLetMapping = "Y" Then
                                sSQL = "SELECT A.Entity,A.CardCode,A.SAPWhseCode,A.SAPBranch,A.OcrCode FROM " & p_oCompDef.sIntDBName & ".dbo.SAP_POS_OUTLET A WHERE A.POSOutletCode = '" & sOutlet & "' "
                                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                oDs = ExecuteSQLQueryDataset(sSQL, p_oCompDef.sIntDBName)
                                If oDs.Tables(0).Rows.Count > 0 Then
                                    sCardCode = oDs.Tables(0).Rows(0)("CardCode").ToString.Trim()
                                    sWhseCode = oDs.Tables(0).Rows(0)("SAPWhseCode").ToString.Trim()
                                    sBranch = oDs.Tables(0).Rows(0)("SAPBranch").ToString.Trim()
                                    sOcrCode = oDs.Tables(0).Rows(0)("OcrCode").ToString.Trim()

                                    sFileId = oDv(0)(0).ToString.Trim()
                                    sPOSNo = oDv(0)(1).ToString.Trim()
                                    sDocDate = oDv(0)(3).ToString.Trim()
                                    Dim iIndex As Integer = sDocDate.IndexOf(" ")
                                    Dim sDate As String = sDocDate.Substring(0, iIndex)
                                    Dim dt As Date
                                    Dim format() = {"dd/MM/yyyy", "d/M/yyyy", "dd-MM-yyyy", "dd.MM.yyyy", "yyyyMMdd", "MMddYYYY", "M/dd/yyyy", "MM/dd/YYYY"}
                                    Date.TryParseExact(sDate, format, System.Globalization.DateTimeFormatInfo.InvariantInfo, Globalization.DateTimeStyles.None, dt)

                                    sPOSDept = oDv(0)(11).ToString.Trim()
                                    sSQL = "SELECT UPPER(OcrCode2) [OcrCode2],SAPBinLoc FROM " & p_oCompDef.sIntDBName & ".dbo.SAP_POS_DEPT WHERE POSOutletCode = '" & sOutlet.ToUpper() & "' AND UPPER(POSDept) = '" & sPOSDept.ToUpper() & "'"
                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                    oDs = New DataSet()
                                    oDs = ExecuteSQLQueryDataset(sSQL, p_oCompDef.sIntDBName)
                                    If oDs.Tables(0).Rows.Count > 0 Then
                                        sOcrCode2 = oDs.Tables(0).Rows(0)("OcrCode2").ToString.Trim()
                                        sWhseBin = oDs.Tables(0).Rows(0)("SAPBinLoc").ToString.Trim()
                                    Else
                                        sOcrCode2 = ""
                                        sWhseBin = ""
                                    End If

                                    sSQL = "SELECT T0.""AbsEntry"" FROM ""OBIN"" T0 WHERE T0.""BinCode"" ='" & sWhseBin & "' "
                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                    oRs.DoQuery(sSQL)
                                    If oRs.RecordCount > 0 Then
                                        sBinLoc = oRs.Fields.Item("AbsEntry").Value
                                    Else
                                        sBinLoc = ""
                                    End If

                                    Dim sCustRefNo As String = String.Empty
                                    Dim oArInvoice As SAPbobsCOM.Documents = Nothing
                                    oArInvoice = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oInvoices)

                                    sCustRefNo = sOutlet & "-" & dt.ToString("yyyyMMdd") & sPOSNo

                                    oArInvoice.CardCode = sCardCode
                                    oArInvoice.DocDate = dt
                                    oArInvoice.NumAtCard = sCustRefNo
                                    oArInvoice.BPL_IDAssignedToInvoice = sBranch
                                    oArInvoice.UserFields.Fields.Item("U_FileID").Value = oDv(0)(0).ToString.Trim()
                                    oArInvoice.UserFields.Fields.Item("U_POSNo").Value = oDv(0)(1).ToString.Trim()
                                    oArInvoice.UserFields.Fields.Item("U_POSOutlet").Value = sOutlet
                                    oArInvoice.UserFields.Fields.Item("U_Covers").Value = oDv(0)(18).ToString.Trim()

                                    Dim iCount As Integer = 1

                                    '********************NEW LOGIC STARTS****************
                                    For j As Integer = 0 To oDv.Count - 1
                                        Dim sItemCode As String = String.Empty
                                        Dim sRevType As String = String.Empty
                                        Dim sDiscType As String = String.Empty
                                        Dim sVatGroup As String = String.Empty

                                        sItemCode = oDv(j)(10).ToString.Trim()
                                        sDiscCode = oDv(j)(12).ToString.Trim()
                                        sSQL = "SELECT ""U_Disc"",""VatGourpSa"" FROM ""OITM"" WHERE ""ItemCode"" = '" & sItemCode & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                        oRs.DoQuery(sSQL)
                                        If oRs.RecordCount > 0 Then
                                            sDiscType = oRs.Fields.Item("U_Disc").Value
                                            sVatGroup = oRs.Fields.Item("VatGourpSa").Value
                                        Else
                                            sDiscType = ""
                                            sVatGroup = ""
                                        End If

                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Disc type for item " & sItemCode & " is " & sDiscType, sFuncName)

                                        If sDiscType.ToUpper() = "Y" Then
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = sItemCode
                                            oArInvoice.Lines.Quantity = CDbl(oDv(j)(16))
                                            oArInvoice.Lines.WarehouseCode = sWhseCode
                                            If Not (sVatGroup = String.Empty) Then
                                                oArInvoice.Lines.VatGroup = sVatGroup
                                            End If
                                            If Not (sOcrCode = String.Empty) Then
                                                oArInvoice.Lines.CostingCode = sOcrCode
                                                oArInvoice.Lines.COGSCostingCode = sOcrCode
                                            End If
                                            If p_oCompDef.sRevDeptMapping = "Y" Then
                                                If Not (sOcrCode2 = String.Empty) Then
                                                    oArInvoice.Lines.CostingCode2 = sOcrCode2
                                                    oArInvoice.Lines.COGSCostingCode2 = sOcrCode2
                                                End If
                                            End If

                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Checking revtype for Disccode " & sDiscCode, sFuncName)

                                            sSQL = "SELECT ""U_RevType"" FROM ""OITM"" WHERE ""ItemCode"" = '" & sDiscCode & "' "
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                            oRs.DoQuery(sSQL)
                                            If oRs.RecordCount > 0 Then
                                                sRevType = oRs.Fields.Item("U_RevType").Value
                                            Else
                                                sRevType = ""
                                            End If

                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Rev type is " & sRevType, sFuncName)

                                            If sRevType.ToUpper() = "ZERO" Then
                                                oArInvoice.Lines.Price = 0
                                                oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Adjustment").Value = oDv(0)(20).ToString.Trim()

                                                If sBinLoc = String.Empty Then
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                                Else
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                                End If
                                                oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                                oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                                oArInvoice.Lines.BinAllocations.Add()

                                                iCount = iCount + 1

                                            ElseIf sRevType.ToUpper() = "FULL" Then
                                                oArInvoice.Lines.Price = CDbl(oDv(j)(15))
                                                oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Adjustment").Value = oDv(0)(20).ToString.Trim()

                                                If sBinLoc = String.Empty Then
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                                Else
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                                End If
                                                oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                                oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                                oArInvoice.Lines.BinAllocations.Add()

                                                iCount = iCount + 1

                                                If iCount > 1 Then
                                                    oArInvoice.Lines.Add()
                                                End If
                                                oArInvoice.Lines.ItemCode = sDiscCode
                                                oArInvoice.Lines.Price = CDbl(oDv(j)(19))
                                                oArInvoice.Lines.Quantity = -1
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = sItemCode
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Adjustment").Value = oDv(0)(20).ToString.Trim()

                                                'If sBinLoc = String.Empty Then
                                                '    oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                                'Else
                                                '    oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                                'End If
                                                'oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                                'oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                                'oArInvoice.Lines.BinAllocations.Add()

                                                iCount = iCount + 1
                                            Else
                                                oArInvoice.Lines.Price = CDbl(oDv(j)(15))
                                                oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Adjustment").Value = oDv(0)(20).ToString.Trim()
                                                iCount = iCount + 1
                                            End If

                                        ElseIf sDiscType.ToUpper() = "N" Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Checking revtype for item " & sItemCode, sFuncName)

                                            sSQL = "SELECT ""U_RevType"" FROM ""OITM"" WHERE ""ItemCode"" = '" & sItemCode & "' "
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                            oRs.DoQuery(sSQL)
                                            If oRs.RecordCount > 0 Then
                                                sRevType = oRs.Fields.Item("U_RevType").Value
                                            Else
                                                sRevType = ""
                                            End If

                                            If sRevType.ToUpper() = "ZERO" Then
                                                Continue For
                                            ElseIf sRevType.ToUpper() = "FULL" Then
                                                If iCount > 1 Then
                                                    oArInvoice.Lines.Add()
                                                End If
                                                oArInvoice.Lines.ItemCode = sItemCode
                                                oArInvoice.Lines.Quantity = CDbl(oDv(j)(16))
                                                oArInvoice.Lines.Price = CDbl(oDv(j)(15))
                                                oArInvoice.Lines.WarehouseCode = sWhseCode
                                                If Not (sVatGroup = String.Empty) Then
                                                    oArInvoice.Lines.VatGroup = sVatGroup
                                                End If
                                                If Not (sOcrCode = String.Empty) Then
                                                    oArInvoice.Lines.CostingCode = sOcrCode
                                                    oArInvoice.Lines.COGSCostingCode = sOcrCode
                                                End If
                                                If p_oCompDef.sRevDeptMapping = "Y" Then
                                                    If Not (sOcrCode2 = String.Empty) Then
                                                        oArInvoice.Lines.CostingCode2 = sOcrCode2
                                                        oArInvoice.Lines.COGSCostingCode2 = sOcrCode2
                                                    End If
                                                End If
                                                oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Adjustment").Value = oDv(0)(20).ToString.Trim()

                                                If sBinLoc = String.Empty Then
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                                Else
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                                End If
                                                oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                                oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                                oArInvoice.Lines.BinAllocations.Add()

                                                iCount = iCount + 1
                                            End If
                                            'ElseIf sDiscCode = "" Then
                                            '    oArInvoice.Lines.Price = CDbl(oDv(j)(15))
                                            '    oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                            '    oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                            '    oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                            '    oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                            '    oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()
                                            '    oArInvoice.Lines.UserFields.Fields.Item("U_Adjustment").Value = oDv(0)(20).ToString.Trim()

                                            '    If sBinLoc = String.Empty Then
                                            '        oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                            '    Else
                                            '        oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                            '    End If
                                            '    oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                            '    oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                            '    oArInvoice.Lines.BinAllocations.Add()

                                            '    iCount = iCount + 1
                                        End If

                                    Next
                                    '********************NEW LOGIC ENDS****************

                                    If Not (oDv(0)(5).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(5)) = 0.0) Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Adding line for service charge", sFuncName)
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sServChargeItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(5))
                                            iCount = iCount + 1
                                        End If
                                    End If
                                    If Not (oDv(0)(7).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(7)) = 0.0) Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Adding line for rounding", sFuncName)
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sRoundingItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(7))
                                            iCount = iCount + 1
                                        End If
                                    End If
                                    If Not (oDv(0)(8).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(8)) = 0.0) Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Adding line for excess", sFuncName)
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sExcessItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(8))
                                            iCount = iCount + 1
                                        End If
                                    End If
                                    If Not (oDv(0)(9).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(9)) = 0.0) Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Adding line for Tips", sFuncName)
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sTippingItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(9))
                                            iCount = iCount + 1
                                        End If
                                    End If

                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Attempting to add ar invoice document", sFuncName)

                                    If oArInvoice.Add() <> 0 Then
                                        sErrDesc = p_oCompany.GetLastErrorDescription
                                        sErrDesc = sErrDesc.Replace("'", " ")

                                        Dim sQuery As String
                                        sQuery = "UPDATE SalesTransHeader SET Status = 'FAIL', ErrorMsg = '" & sErrDesc & "', SAPSyncDate = GETDATE() " & _
                                                 " WHERE FileID = '" & sFileId & "' AND POSOutlet = '" & sOutlet & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sQuery, sFuncName)
                                        If ExecuteNonQuery(sQuery, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                        Console.WriteLine("Error while adding A/R invoice document/ " & sErrDesc)
                                        Throw New ArgumentException(sErrDesc)
                                    Else
                                        Dim iDocEntry As Integer
                                        iDocEntry = p_oCompany.GetNewObjectKey()
                                        System.Runtime.InteropServices.Marshal.ReleaseComObject(oArInvoice)

                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("A/R invoice created successfully. DocEntry is " & iDocEntry, sFuncName)

                                        Console.WriteLine("A/R invoice document created successfully. DocEntry is :: " & iDocEntry)

                                        Dim sQuery As String
                                        sQuery = "UPDATE SalesTransHeader SET Status = 'SUCCESS', ErrorMsg = '', SAPSyncDate = GETDATE(), ARDocEntry = '" & iDocEntry & "' " & _
                                                 " WHERE FileID = '" & sFileId & "' AND POSOutlet = '" & sOutlet & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sQuery, sFuncName)
                                        If ExecuteNonQuery(sQuery, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Creating entry in Header backup table ", sFuncName)
                                        sQuery = "INSERT INTO SalesTransHeader_Backup(FileID,POSNo,POSOutlet,DocDate,TotalGrossAmt,SvcCharge,GST,Rounding,Excess,Tips,Covers,RUpdated) " & _
                                                 " VALUES('" & oDv(0)(0).ToString.Trim() & "','" & oDv(0)(1).ToString.Trim() & "','" & oDv(0)(2).ToString.Trim() & "', " & _
                                                 " '" & oDv(0)(3).ToString.Trim() & "','" & oDv(0)(4).ToString.Trim() & "','" & oDv(0)(5).ToString.Trim() & "','" & oDv(0)(6).ToString.Trim() & "', " & _
                                                 " '" & oDv(0)(7).ToString.Trim() & "','" & oDv(0)(8).ToString.Trim() & "','" & oDv(0)(9).ToString.Trim() & "','" & oDv(0)(18).ToString.Trim() & "','1') "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sQuery, sFuncName)
                                        If ExecuteNonQuery(sQuery, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                        sQuery = String.Empty
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Creating entry in Transaction backup table ", sFuncName)
                                        For j As Integer = 0 To oDv.Count - 1
                                            If sQuery = "" Then
                                                sQuery = "INSERT INTO SalesTransDetails_Backup VALUES('" & oDv(j)(0).ToString.Trim() & "','" & oDv(j)(3).ToString.Trim & "','" & oDv(j)(1).ToString.Trim() & "', " & _
                                                         " '" & oDv(j)(2).ToString.Trim() & "','" & oDv(j)(11).ToString.Trim() & "','" & oDv(j)(10).ToString.Trim() & "','" & oDv(j)(12).ToString.Trim() & "', " & _
                                                         " '" & oDv(j)(13).ToString.Trim() & "', '" & CDbl(oDv(j)(19)) & "','" & oDv(j)(20).ToString.Trim() & "','" & oDv(j)(14).ToString.Trim() & "', " & _
                                                         " '" & CDbl(oDv(j)(15)) & "','" & CDbl(oDv(j)(16)) & "','" & CDbl(oDv(j)(17)) & "','1'); "
                                            Else
                                                sQuery = sQuery & " INSERT INTO SalesTransDetails_Backup VALUES('" & oDv(j)(0).ToString.Trim() & "','" & oDv(j)(3).ToString.Trim & "','" & oDv(j)(1).ToString.Trim() & "', " & _
                                                         " '" & oDv(j)(2).ToString.Trim() & "','" & oDv(j)(11).ToString.Trim() & "','" & oDv(j)(10).ToString.Trim() & "','" & oDv(j)(12).ToString.Trim() & "', " & _
                                                         " '" & oDv(j)(13).ToString.Trim() & "', '" & CDbl(oDv(j)(19)) & "','" & oDv(j)(20).ToString.Trim() & "','" & oDv(j)(14).ToString.Trim() & "', " & _
                                                         " '" & CDbl(oDv(j)(15)) & "','" & CDbl(oDv(j)(16)) & "','" & CDbl(oDv(j)(17)) & "','1'); "
                                            End If

                                        Next
                                        If sQuery <> "" Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sQuery, sFuncName)
                                            If ExecuteNonQuery(sQuery, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                        End If

                                    End If
                                End If

                            End If

                        End If

                    End If
                End If
            Next
            System.Runtime.InteropServices.Marshal.ReleaseComObject(oRs)

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            CreateARInvoice = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)

            System.Runtime.InteropServices.Marshal.ReleaseComObject(oRs)

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            CreateARInvoice = RTN_ERROR
        End Try
    End Function

    Private Function CreateARInvoice_Old(ByVal oDv As DataView, ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "CreateARInvoice_Old"
        Dim sSQL As String = String.Empty
        Dim sFileId As String = String.Empty
        Dim sCardCode As String = String.Empty
        Dim sPOSDept As String = String.Empty
        Dim sOutlet As String = String.Empty
        Dim sBranch As String = String.Empty
        Dim sSAPOutlet As String = String.Empty
        Dim sOcrCode As String = String.Empty
        Dim sOcrCode2 As String = String.Empty
        Dim sWhseCode As String = String.Empty
        Dim sWhseBin As String = String.Empty
        Dim sDocDate As String = String.Empty
        Dim sDiscCode As String = String.Empty
        Dim sVatGroup As String = String.Empty
        Dim sBinLoc As String = String.Empty
        Dim oDs As New DataSet

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function", sFuncName)

            Dim oDtGroup As DataTable = oDv.Table.DefaultView.ToTable(True, "FileID")
            For i As Integer = 0 To oDtGroup.Rows.Count - 1
                If Not (oDtGroup.Rows(i).Item(0).ToString.Trim() = String.Empty Or oDtGroup.Rows(i).Item(0).ToString.ToUpper().Trim() = "FILEID") Then
                    oDv.RowFilter = "FileID = '" & oDtGroup.Rows(i).Item(0).ToString.Trim() & "' "
                    sFileId = oDtGroup.Rows(i).Item(0).ToString.Trim()
                    If oDv.Count > 0 Then
                        Dim oDt As New DataTable
                        oDt = oDv.ToTable()
                        Dim oInvDv As DataView = New DataView(oDt)

                        sOutlet = oDv(0)(2).ToString().Trim()

                        If oInvDv.Count > 0 Then

                            Dim oCollectionsDt As New DataTable
                            oCollectionsDt = oInvDv.ToTable()
                            Dim oCollectionsDv As DataView = New DataView(oCollectionsDt)

                            If p_oCompDef.sOutLetMapping = "Y" Then
                                sSQL = "SELECT A.Entity,A.CardCode,A.SAPWhseCode,A.SAPBranch,A.OcrCode FROM SAP_POS_OUTLET A WHERE A.POSOutletCode = '" & sOutlet & "' "
                                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                oDs = ExecuteSQLQueryDataset(sSQL, p_oCompDef.sIntDBName)
                                If oDs.Tables(0).Rows.Count > 0 Then
                                    sCardCode = oDs.Tables(0).Rows(0)("CardCode").ToString.Trim()
                                    sWhseCode = oDs.Tables(0).Rows(0)("SAPWhseCode").ToString.Trim()
                                    sBranch = oDs.Tables(0).Rows(0)("SAPBranch").ToString.Trim()
                                    sOcrCode = oDs.Tables(0).Rows(0)("OcrCode").ToString.Trim()

                                    sFileId = oDv(0)(0).ToString.Trim()
                                    sDocDate = oDv(0)(3).ToString.Trim()
                                    Dim iIndex As Integer = sDocDate.IndexOf(" ")
                                    Dim sDate As String = sDocDate.Substring(0, iIndex)
                                    Dim dt As Date
                                    Dim format() = {"dd/MM/yyyy", "d/M/yyyy", "dd-MM-yyyy", "dd.MM.yyyy", "yyyyMMdd", "MMddYYYY", "M/dd/yyyy", "MM/dd/YYYY"}
                                    Date.TryParseExact(sDate, format, System.Globalization.DateTimeFormatInfo.InvariantInfo, Globalization.DateTimeStyles.None, dt)

                                    sPOSDept = oDv(0)(11).ToString.Trim()
                                    sSQL = "SELECT UPPER(OcrCode2) [OcrCode2],SAPBinLoc FROM SAP_POS_DEPT WHERE POSOutletCode = '" & sOutlet.ToUpper() & "' AND UPPER(POSDept) = '" & sPOSDept.ToUpper() & "'"
                                    oDs = New DataSet()
                                    oDs = ExecuteSQLQueryDataset(sSQL, p_oCompDef.sIntDBName)
                                    If oDs.Tables(0).Rows.Count > 0 Then
                                        sOcrCode2 = oDs.Tables(0).Rows(0)("OcrCode2").ToString.Trim()
                                        sWhseBin = oDs.Tables(0).Rows(0)("SAPBinLoc").ToString.Trim()
                                    End If

                                    sSQL = "SELECT T0.[AbsEntry] FROM OBIN T0 with (nolock) WHERE T0.[BinCode] ='" & sWhseBin & "' "
                                    oDs = New DataSet()
                                    oDs = ExecuteSQLQueryDataset(sSQL, p_oCompany.CompanyDB)
                                    If oDs.Tables(0).Rows.Count > 0 Then
                                        sBinLoc = oDs.Tables(0).Rows(0)("AbsEntry").ToString.Trim()
                                    End If

                                    Dim oArInvoice As SAPbobsCOM.Documents = Nothing
                                    oArInvoice = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oInvoices)

                                    oArInvoice.CardCode = sCardCode
                                    oArInvoice.DocDate = dt
                                    oArInvoice.BPL_IDAssignedToInvoice = sBranch
                                    oArInvoice.UserFields.Fields.Item("U_FileID").Value = oDv(0)(0).ToString.Trim()
                                    oArInvoice.UserFields.Fields.Item("U_POSNo").Value = oDv(0)(1).ToString.Trim()
                                    oArInvoice.UserFields.Fields.Item("U_POSOutlet").Value = sOutlet
                                    'oArInvoice.UserFields.Fields.Item("U_Outlet").Value = sOutlet
                                    oArInvoice.UserFields.Fields.Item("U_Covers").Value = oDv(0)(18).ToString.Trim()

                                    Dim iCount As Integer = 1
                                    For j As Integer = 0 To oDv.Count - 1
                                        Dim sItemCode As String = String.Empty
                                        Dim sRevType As String = String.Empty
                                        Dim sDiscType As String = String.Empty

                                        sItemCode = oDv(j)(10).ToString.Trim()
                                        sDiscCode = oDv(j)(12).ToString.Trim()

                                        sSQL = "SELECT U_Disc,VatGourpSa FROM OITM WHERE ItemCode = '" & sItemCode & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                        oDs = ExecuteSQLQueryDataset(sSQL, p_oCompany.CompanyDB)
                                        If oDs.Tables(0).Rows.Count > 0 Then
                                            sDiscType = oDs.Tables(0).Rows(0)("U_Disc").ToString.Trim()
                                            sVatGroup = oDs.Tables(0).Rows(0)("VatGourpSa").ToString.Trim()
                                        End If
                                        If sDiscType.ToUpper() = "N" Then
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = sItemCode
                                            oArInvoice.Lines.Quantity = CDbl(oDv(j)(16))
                                            oArInvoice.Lines.WarehouseCode = sWhseCode
                                            If Not (sVatGroup = String.Empty) Then
                                                oArInvoice.Lines.VatGroup = sVatGroup
                                            End If
                                            If Not (sOcrCode = String.Empty) Then
                                                oArInvoice.Lines.CostingCode = sOcrCode
                                                oArInvoice.Lines.COGSCostingCode = sOcrCode
                                            End If
                                            If p_oCompDef.sRevDeptMapping = "Y" Then
                                                If Not (sOcrCode2 = String.Empty) Then
                                                    oArInvoice.Lines.CostingCode2 = sOcrCode2
                                                    oArInvoice.Lines.COGSCostingCode2 = sOcrCode2
                                                End If
                                            End If

                                            sSQL = "SELECT U_RevType FROM OITM WHERE ItemCode = '" & sDiscCode & "' "
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                            oDs = New DataSet()
                                            oDs = ExecuteSQLQueryDataset(sSQL, p_oCompany.CompanyDB)
                                            If oDs.Tables(0).Rows.Count > 0 Then
                                                sRevType = oDs.Tables(0).Rows(0)("U_RevType").ToString.Trim()
                                            End If
                                            If sRevType.ToUpper() = "ZERO" Then
                                                oArInvoice.Lines.Price = 0
                                            ElseIf sRevType.ToUpper() = "FULL" Then
                                                oArInvoice.Lines.Price = CDbl(oDv(j)(15))
                                            End If
                                            oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                            oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                            oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                            oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                            oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()

                                            If sBinLoc = String.Empty Then
                                                oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                            Else
                                                oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                            End If
                                            oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                            oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                            oArInvoice.Lines.BinAllocations.Add()

                                            iCount = iCount + 1
                                        ElseIf sDiscType.ToUpper() = "Y" Then
                                            sSQL = "SELECT U_RevType FROM OITM WHERE ItemCode = '" & sItemCode & "' "
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                            oDs = New DataSet
                                            oDs = ExecuteSQLQueryDataset(sSQL, p_oCompany.CompanyDB)
                                            If oDs.Tables(0).Rows.Count > 0 Then
                                                sRevType = oDs.Tables(0).Rows(0)("U_RevType").ToString.Trim()
                                            End If
                                            If sRevType.ToUpper() = "ZERO" Then
                                                Continue For
                                            ElseIf sRevType.ToUpper() = "FULL" Then
                                                If iCount > 1 Then
                                                    oArInvoice.Lines.Add()
                                                End If
                                                oArInvoice.Lines.ItemCode = sItemCode
                                                oArInvoice.Lines.Quantity = CDbl(oDv(j)(16))
                                                oArInvoice.Lines.Price = CDbl(oDv(j)(15))
                                                oArInvoice.Lines.WarehouseCode = sWhseCode
                                                If Not (sVatGroup = String.Empty) Then
                                                    oArInvoice.Lines.VatGroup = sVatGroup
                                                End If
                                                If Not (sOcrCode = String.Empty) Then
                                                    oArInvoice.Lines.CostingCode = sOcrCode
                                                    oArInvoice.Lines.COGSCostingCode = sOcrCode
                                                End If
                                                If p_oCompDef.sRevDeptMapping = "Y" Then
                                                    If Not (sOcrCode2 = String.Empty) Then
                                                        oArInvoice.Lines.CostingCode2 = sOcrCode2
                                                        oArInvoice.Lines.COGSCostingCode2 = sOcrCode2
                                                    End If
                                                End If
                                                oArInvoice.Lines.LineTotal = CDbl(oDv(j)(17))
                                                oArInvoice.Lines.UserFields.Fields.Item("U_Dept").Value = oDv(j)(11).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscCode").Value = oDv(j)(12).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_DiscItem").Value = oDv(j)(13).ToString.Trim()
                                                oArInvoice.Lines.UserFields.Fields.Item("U_SetMealCode").Value = oDv(j)(14).ToString.Trim()

                                                If sBinLoc = String.Empty Then
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = 1
                                                Else
                                                    oArInvoice.Lines.BinAllocations.BinAbsEntry = sBinLoc
                                                End If
                                                oArInvoice.Lines.BinAllocations.Quantity = CDbl(oDv(j)(16))
                                                oArInvoice.Lines.BinAllocations.AllowNegativeQuantity = SAPbobsCOM.BoYesNoEnum.tYES
                                                oArInvoice.Lines.BinAllocations.Add()

                                                iCount = iCount + 1
                                            End If
                                        End If

                                    Next
                                    If Not (oDv(0)(5).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(5)) = 0.0) Then
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sServChargeItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(5))
                                            iCount = iCount + 1
                                        End If
                                    End If
                                    If Not (oDv(0)(7).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(7)) = 0.0) Then
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sRoundingItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(7))
                                            iCount = iCount + 1
                                        End If
                                    End If
                                    If Not (oDv(0)(8).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(8)) = 0.0) Then
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sExcessItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(8))
                                            iCount = iCount + 1
                                        End If
                                    End If
                                    If Not (oDv(0)(9).ToString.Trim() = String.Empty) Then
                                        If Not (CDbl(oDv(0)(9)) = 0.0) Then
                                            If iCount > 1 Then
                                                oArInvoice.Lines.Add()
                                            End If
                                            oArInvoice.Lines.ItemCode = p_oCompDef.sTippingItem
                                            oArInvoice.Lines.Quantity = 1
                                            oArInvoice.Lines.Price = CDbl(oDv(0)(9))
                                            iCount = iCount + 1
                                        End If
                                    End If

                                    If oArInvoice.Add() <> 0 Then
                                        sErrDesc = p_oCompany.GetLastErrorDescription
                                        sErrDesc = sErrDesc.Replace("'", " ")

                                        Dim sQuery As String
                                        sQuery = "UPDATE SalesTransHeader SET Status = 'FAIL', ErrorMsg = '" & sErrDesc & "', SAPSyncDate = GETDATE() " & _
                                                 " WHERE FileID = '" & sFileId & "' AND POSOutlet = '" & sOutlet & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sQuery, sFuncName)
                                        If ExecuteNonQuery(sQuery, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                        Console.WriteLine("Error while adding A/R invoice document/ " & sErrDesc)
                                        Throw New ArgumentException(sErrDesc)
                                    Else
                                        Dim iDocEntry As Integer
                                        iDocEntry = p_oCompany.GetNewObjectKey()
                                        System.Runtime.InteropServices.Marshal.ReleaseComObject(oArInvoice)

                                        Dim sQuery As String
                                        sQuery = "UPDATE SalesTransHeader SET Status = 'SUCCESS', ErrorMsg = '', SAPSyncDate = GETDATE(), ARDocEntry = '" & iDocEntry & "' " & _
                                                 " WHERE FileID = '" & sFileId & "' AND POSOutlet = '" & sOutlet & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sQuery, sFuncName)
                                        If ExecuteNonQuery(sQuery, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                    End If
                                End If
                            End If

                        End If

                    End If
                End If
            Next

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            CreateARInvoice_Old = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            CreateARInvoice_Old = RTN_ERROR
        End Try
    End Function

    Private Function CreateCollectionEntry(ByVal sOutlet As String, ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "CreateCollectionEntry"
        Dim sSQL As String = String.Empty
        Dim sFileId As String = String.Empty
        Dim sPOSNo As String = String.Empty
        Dim sPOSDept As String = String.Empty
        Dim sDocDate As String = String.Empty
        Dim oDs As New DataSet
        Dim bLineAdded As Boolean = False
        Dim odt_Collections As DataTable
        Dim oRs As SAPbobsCOM.Recordset
        oRs = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.BoRecordset)

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function ", sFuncName)

            '********************************************************
            sSQL = "SELECT A.FileID,A.POSOutlet,A.POSNo,CONVERT(CHAR,A.BusinessDate,103) BusinessDate,A.PaymentCode,ISNULL(PaymentAmt,0) [PaymentAmt],A.BatchCode,B.ARDocEntry " & _
                    " FROM " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails A " & _
                    " INNER JOIN " & p_oCompDef.sIntDBName & ".dbo.SalesTransHeader B ON B.FileID = A.FileID AND B.POSOutlet = A.POSOutlet AND B.POSNo = A.POSNo " & _
                    " WHERE ISNULL(A.RCDocEntry,'') = '' AND ISNULL(B.ARDocEntry,'') <> '' AND A.RUpdated = 1 "
            odt_Collections = ExecuteQueryReturnDataTable(sSQL, p_oCompDef.sIntDBName)
            odt_Collections.Columns.Add("CardCode", GetType(String))

            For intRow As Integer = 0 To odt_Collections.Rows.Count - 1
                If Not (odt_Collections.Rows(intRow).Item(0).ToString.Trim() = String.Empty Or odt_Collections.Rows(intRow).Item(0).ToString.ToUpper().Trim() = "FILEID") Then
                    Dim sArDocEntry As String = odt_Collections.Rows(intRow).Item(7).ToString

                    sSQL = "SELECT ""CardCode"" FROM ""OINV"" WHERE ""DocEntry"" = '" & sArDocEntry & "'"
                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                    oRs.DoQuery(sSQL)
                    If oRs.RecordCount > 0 Then
                        odt_Collections.Rows(intRow)("CardCode") = oRs.Fields.Item("CardCode").Value
                    End If

                End If
            Next

            Dim oDvCollections As DataView = New DataView(odt_Collections)

            If oDvCollections.Count > 0 Then
                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Grouping datas based on Fileid and cardcode", sFuncName)
                Dim oDtGroup As DataTable = oDvCollections.Table.DefaultView.ToTable(True, "FileID", "CardCode")
                For i As Integer = 0 To oDtGroup.Rows.Count - 1
                    If Not (oDtGroup.Rows(i).Item(0).ToString.Trim() = String.Empty Or oDtGroup.Rows(i).Item(0).ToString.ToUpper().Trim() = "FILEID") Then
                        oDvCollections.RowFilter = "FileID = '" & oDtGroup.Rows(i).Item(0).ToString.Trim() & "' AND CardCode = '" & oDtGroup.Rows(i).Item(1).ToString.Trim() & "' "

                        sFileId = oDtGroup.Rows(i).Item(0).ToString.Trim()
                        If oDvCollections.Count > 0 Then
                            Dim oDt_Coll_New As DataTable
                            oDt_Coll_New = oDvCollections.ToTable()
                            Dim oDv As DataView = New DataView(oDt_Coll_New)

                            If oDv.Count > 0 Then
                                Dim oPayments As SAPbobsCOM.IPayments = Nothing
                                oPayments = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oIncomingPayments)
                                oPayments.DocType = SAPbobsCOM.BoRcptTypes.rCustomer

                                sDocDate = oDv(0)(3).ToString.Trim()
                                Dim sDate As String
                                Dim iIndex As Integer = sDocDate.IndexOf(" ")
                                If iIndex > -1 Then
                                    sDate = sDocDate.Substring(0, iIndex)
                                Else
                                    sDate = sDocDate
                                End If
                                Dim dtDocDate As Date
                                Dim format() = {"dd/MM/yyyy", "d/M/yyyy", "dd-MM-yyyy", "dd.MM.yyyy", "yyyyMMdd", "MMddYYYY", "M/dd/yyyy", "MM/dd/YYYY"}
                                Date.TryParseExact(sDate, format, System.Globalization.DateTimeFormatInfo.InvariantInfo, Globalization.DateTimeStyles.None, dtDocDate)

                                oPayments.CardCode = oDv(0)(8).ToString.Trim()
                                oPayments.DocDate = dtDocDate
                                oPayments.UserFields.Fields.Item("U_FileID").Value = oDv(0)(0).ToString.Trim()
                                oPayments.UserFields.Fields.Item("U_POSOutlet").Value = oDv(0)(1).ToString.Trim()
                                oPayments.UserFields.Fields.Item("U_POSNo").Value = oDv(0)(2).ToString.Trim()
                                oPayments.UserFields.Fields.Item("U_Outlet").Value = sOutlet

                                If p_oCompDef.sOutLetMapping = "Y" Then
                                    sSQL = "SELECT A.SAPBranch FROM " & p_oCompDef.sIntDBName & ".dbo.SAP_POS_OUTLET A WHERE A.POSOutletCode = '" & sOutlet & "' "
                                    If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                    oDs = New DataSet()
                                    oDs = ExecuteSQLQueryDataset(sSQL, p_oCompDef.sIntDBName)
                                    If oDs.Tables(0).Rows.Count > 0 Then
                                        oPayments.BPLID = oDs.Tables(0).Rows(0)("SAPBranch").ToString.Trim()
                                    End If
                                End If

                                Console.WriteLine("Assigning Invoices")
                                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Assinging invoice to payment", sFuncName)

                                Dim sDocEntry As String = String.Empty
                                For j As Integer = 0 To oDv.Count - 1
                                    If j = 0 Then
                                        sDocEntry = oDv(j)(7).ToString.Trim()
                                    ElseIf j > 0 Then
                                        If sDocEntry = oDv(j)(7).ToString.Trim() Then
                                            Continue For
                                        Else
                                            sDocEntry = oDv(j)(7).ToString.Trim()
                                        End If
                                    End If
                                    oPayments.Invoices.DocEntry = oDv(j)(7).ToString.Trim()
                                    oPayments.Invoices.InvoiceType = SAPbobsCOM.BoRcptInvTypes.it_Invoice
                                    oPayments.Invoices.DocLine = 0
                                    'oPayments.Invoices.SumApplied = oRecSet.Fields.Item("PaymentAmt").Value
                                    bLineAdded = True
                                Next

                                Console.WriteLine("Selecting Payment methods")
                                If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Selecting Payment methods", sFuncName)

                                For j As Integer = 0 To oDv.Count - 1
                                    sSQL = "SELECT T0.""CreditCard"" FROM ""OCRC"" T0 WHERE T0.""CardName"" ='" & oDv(j)(4).ToString.Trim() & "'"
                                    oRs.DoQuery(sSQL)
                                    If oRs.RecordCount > 0 Then
                                        oPayments.CreditCards.CreditCard = oRs.Fields.Item("CreditCard").Value
                                        oPayments.CreditCards.CreditCardNumber = oDv(j)(2).ToString.Trim()
                                        oPayments.CreditCards.CreditSum = oDv(j)(5).ToString.Trim()
                                        Dim sCrdtValidDt As Date = "9999-12-01"
                                        oPayments.CreditCards.CardValidUntil = sCrdtValidDt
                                        If Not (oDv(j)(6).ToString.Trim() = String.Empty) Then
                                            oPayments.CreditCards.VoucherNum = oDv(j)(6).ToString.Trim()
                                        Else
                                            oPayments.CreditCards.VoucherNum = oDv(j)(2).ToString.Trim() & "-" & DateTime.Now.ToString("yyyyMMdd")
                                        End If
                                        oPayments.CreditCards.Add()
                                    End If
                                Next

                                If bLineAdded = True Then
                                    Console.WriteLine("Attempting to add payment document")
                                    If oPayments.Add() <> 0 Then
                                        sErrDesc = p_oCompany.GetLastErrorDescription()
                                        Throw New ArgumentException(sErrDesc)
                                    Else
                                        Dim iDocEntry As Integer
                                        p_oCompany.GetNewObjectCode(iDocEntry)

                                        Console.WriteLine("Payment document successfully created. DocEntry is :: " & iDocEntry)
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Payment document successfully created. DocEntry is :: " & iDocEntry, sFuncName)

                                        sSQL = "UPDATE " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails SET Status = 'SUCCESS', ErrorMsg = '', SAPSyncDate = GETDATE(), RCDocEntry = '" & iDocEntry & "' " & _
                                                 " WHERE FileID = '" & sFileId & "' AND POSOutlet = '" & sOutlet & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                        If ExecuteNonQuery(sSQL, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)

                                        oDv.RowFilter = Nothing
                                        oDv.RowFilter = "FileID = '" & sFileId & "'"
                                        sSQL = String.Empty

                                        For j As Integer = 0 To oDv.Count - 1
                                            sDocDate = String.Empty
                                            sDocDate = oDv(j)(3).ToString.Trim()
                                            Dim sBusinessDate As String
                                            Dim iBDIndex As Integer = sDocDate.IndexOf(" ")
                                            If iBDIndex > -1 Then
                                                sBusinessDate = sDocDate.Substring(0, iIndex)
                                            Else
                                                sBusinessDate = sDocDate
                                            End If
                                            Dim dtBusinessDate As Date

                                            Dim formatBD() = {"dd/MM/yyyy", "d/M/yyyy", "dd-MM-yyyy", "dd.MM.yyyy", "yyyyMMdd", "MMddYYYY", "M/dd/yyyy", "MM/dd/YYYY"}
                                            Date.TryParseExact(sBusinessDate, format, System.Globalization.DateTimeFormatInfo.InvariantInfo, Globalization.DateTimeStyles.None, dtBusinessDate)

                                            If sSQL = "" Then
                                                sSQL = "INSERT INTO " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails_Backup(FileId,POSOutlet,POSNo,BusinessDate,PaymentCode,PaymentAmt,BatchCode,RUpdated) " & _
                                                       " VALUES ('" & oDv(j)(0).ToString.Trim() & "','" & oDv(j)(1).ToString.Trim() & "','" & oDv(j)(2).ToString.Trim() & "', " & _
                                                       " '" & dtBusinessDate.ToString("yyyy-MM-dd") & "','" & oDv(j)(4).ToString.Trim() & "','" & oDv(j)(5).ToString.Trim() & "', " & _
                                                       " '" & oDv(j)(6).ToString.Trim() & "','1'); "
                                            Else
                                                sSQL = sSQL & "INSERT INTO " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails_Backup(FileId,POSOutlet,POSNo,BusinessDate,PaymentCode,PaymentAmt,BatchCode,RUpdated) " & _
                                                       " VALUES ('" & oDv(j)(0).ToString.Trim() & "','" & oDv(j)(1).ToString.Trim() & "','" & oDv(j)(2).ToString.Trim() & "', " & _
                                                       " '" & dtBusinessDate.ToString.Trim() & "','" & oDv(j)(4).ToString.Trim() & "','" & oDv(j)(5).ToString.Trim() & "', " & _
                                                       " '" & oDv(j)(6).ToString.Trim() & "','1'); "
                                            End If
                                        Next
                                        If sSQL <> "" Then
                                            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                            If ExecuteNonQuery(sSQL, sErrDesc) <> RTN_SUCCESS Then Throw New ArgumentException(sErrDesc)
                                        End If

                                    End If
                                End If
                            End If


                        End If

                    End If
                Next
            End If

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            CreateCollectionEntry = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            CreateCollectionEntry = RTN_ERROR
        End Try
    End Function

    Private Function CreateCollectionEntry_baCKUP(ByVal oDv As DataView, ByRef sErrDesc As String) As Long
        Dim sFuncName As String = "CreateCollectionEntry_baCKUP"
        Dim sSQL As String = String.Empty
        Dim sFileId As String = String.Empty
        Dim sPOSNo As String = String.Empty
        Dim sPOSDept As String = String.Empty
        Dim sOutlet As String = String.Empty
        Dim sBranch As String = String.Empty
        Dim sSAPOutlet As String = String.Empty
        Dim sDocDate As String = String.Empty
        Dim oDs As New DataSet
        Dim bLineAdded As Boolean = False
        Dim oRecordSet As SAPbobsCOM.Recordset = Nothing
        Dim oRecSet As SAPbobsCOM.Recordset = Nothing
        Dim oRs As SAPbobsCOM.Recordset = Nothing
        oRecordSet = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.BoRecordset)
        oRecSet = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.BoRecordset)
        oRs = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.BoRecordset)

        Try
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Starting Function ", sFuncName)

            Dim oDtGroup As DataTable = oDv.Table.DefaultView.ToTable(True, "FileID")
            For i As Integer = 0 To oDtGroup.Rows.Count - 1
                If Not (oDtGroup.Rows(i).Item(0).ToString.Trim() = String.Empty Or oDtGroup.Rows(i).Item(0).ToString.ToUpper().Trim() = "FILEID") Then
                    oDv.RowFilter = "FileID = '" & oDtGroup.Rows(i).Item(0).ToString.Trim() & "' "
                    sFileId = oDtGroup.Rows(i).Item(0).ToString.Trim()

                    If oDv.Count > 0 Then
                        Dim dt As DataTable
                        dt = oDv.ToTable()
                        Dim oDv_Collections As DataView = New DataView(dt)
                        If oDv.Count > 0 Then
                            Dim oPayments As SAPbobsCOM.IPayments = Nothing
                            oPayments = p_oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oIncomingPayments)
                            oPayments.DocType = SAPbobsCOM.BoRcptTypes.rCustomer

                            Dim sARDocEntry As String = String.Empty
                            Dim sCardCode As String = String.Empty
                            Dim sQuery As String = String.Empty

                            sOutlet = oDv(0)(1).ToString.Trim()

                            sSQL = "SELECT DISTINCT CardCode FROM " & p_oCompany.CompanyDB & ".dbo.OINV WHERE DocEntry IN (SELECT B.ARDocEntry FROM " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails A " & _
                                   "        INNER JOIN " & p_oCompDef.sIntDBName & ".dbo.SalesTransHeader B ON B.FileID = A.FileID AND B.POSOutlet = A.POSOutlet AND B.POSNo = A.POSNo " & _
                                   "        WHERE ISNULL(A.RCDocEntry,'') = '' AND ISNULL(B.ARDocEntry,'') <> '' AND A.RUpdated = 1) "
                            oRecordSet.DoQuery(sSQL)
                            If Not (oRecordSet.BoF And oRecordSet.EoF) Then
                                oRecordSet.MoveFirst()
                                Do Until oRecordSet.EoF
                                    sCardCode = oRecordSet.Fields.Item("CardCode").Value

                                    sQuery = "SELECT A.FileID,A.POSOutlet,A.POSNo,CONVERT(CHAR,A.BusinessDate,103) BusinessDate,A.PaymentCode,ISNULL(PaymentAmt,0) [PaymentAmt],A.BatchCode,B.ARDocEntry " & _
                                             " FROM " & p_oCompDef.sIntDBName & ".dbo.CollectionDetails A " & _
                                             " INNER JOIN " & p_oCompDef.sIntDBName & ".dbo.SalesTransHeader B ON B.FileID = A.FileID AND B.POSOutlet = A.POSOutlet AND B.POSNo = A.POSNo " & _
                                             " INNER JOIN " & p_oCompany.CompanyDB & ".dbo.OINV C ON C.DocEntry = B.ARDocEntry " & _
                                             " WHERE ISNULL(A.RCDocEntry,'') = '' AND ISNULL(B.ARDocEntry,'') <> '' AND A.RUpdated = 1 AND C.CardCode = '" & sCardCode & "' "
                                    oRecSet.DoQuery(sQuery)
                                    If Not (oRecSet.BoF And oRecSet.EoF) Then
                                        oRecSet.MoveFirst()

                                        sDocDate = oRecSet.Fields.Item("BusinessDate").Value
                                        Dim iIndex As Integer = sDocDate.IndexOf(" ")
                                        Dim sDate As String = sDocDate.Substring(0, iIndex)
                                        Dim dtDocDate As Date
                                        Dim format() = {"dd/MM/yyyy", "d/M/yyyy", "dd-MM-yyyy", "dd.MM.yyyy", "yyyyMMdd", "MMddYYYY", "M/dd/yyyy", "MM/dd/YYYY"}
                                        Date.TryParseExact(sDate, format, System.Globalization.DateTimeFormatInfo.InvariantInfo, Globalization.DateTimeStyles.None, dtDocDate)

                                        oPayments.CardCode = sCardCode
                                        oPayments.DocDate = dtDocDate
                                        oPayments.UserFields.Fields.Item("U_FileID").Value = oRecSet.Fields.Item("FileID").Value
                                        oPayments.UserFields.Fields.Item("U_POSOutlet").Value = oRecSet.Fields.Item("POSOutlet").Value
                                        oPayments.UserFields.Fields.Item("U_POSNo").Value = oRecSet.Fields.Item("POSNo").Value
                                        oPayments.UserFields.Fields.Item("U_Outlet").Value = oRecSet.Fields.Item("POSNo").Value

                                        sSQL = "SELECT A.SAPBranch FROM " & p_oCompDef.sIntDBName & ".dbo.SAP_POS_OUTLET A WHERE A.POSOutletCode = '" & sOutlet & "' "
                                        If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Executing SQL " & sSQL, sFuncName)
                                        oRs.DoQuery(sSQL)
                                        If oRs.RecordCount > 0 Then
                                            sBranch = oRs.Fields.Item("SAPBranch").Value
                                        End If

                                        If p_oCompDef.sOutLetMapping = "Y" Then
                                            oPayments.BPLID = sBranch
                                        End If

                                        Do Until oRecSet.EoF
                                            oPayments.Invoices.DocEntry = sARDocEntry
                                            oPayments.Invoices.InvoiceType = SAPbobsCOM.BoRcptInvTypes.it_Invoice
                                            oPayments.Invoices.DocLine = 0
                                            'oPayments.Invoices.SumApplied = oRecSet.Fields.Item("PaymentAmt").Value
                                            bLineAdded = True
                                            oRecSet.MoveNext()
                                        Loop
                                        oRecSet.MoveFirst()
                                        Do Until oRecSet.EoF
                                            oPayments.CreditCards.Add()
                                            oPayments.CreditCards.CreditCard = oRecSet.Fields.Item("PaymentCode").Value
                                            oPayments.CreditCards.CreditSum = oRecSet.Fields.Item("PaymentAmt").Value
                                            oPayments.CreditCards.VoucherNum = oRecSet.Fields.Item("BatchCode").Value
                                            oRecSet.MoveNext()
                                        Loop


                                    End If

                                    If bLineAdded = True Then
                                        If oPayments.Add() <> 0 Then
                                            sErrDesc = p_oCompany.GetLastErrorDescription
                                            Throw New ArgumentException(sErrDesc)
                                        Else
                                            Dim iDocEntry As Integer
                                            p_oCompany.GetNewObjectCode(iDocEntry)
                                            Console.WriteLine("Payment Created successfully :: " & iDocEntry)

                                            sSQL = "UPDATE CollectionDetails SET"
                                        End If
                                    End If

                                    oRecordSet.MoveNext()
                                Loop
                            End If

                        End If
                    End If
                End If
            Next

            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with SUCCESS", sFuncName)
            CreateCollectionEntry_baCKUP = RTN_SUCCESS
        Catch ex As Exception
            sErrDesc = ex.Message
            Call WriteToLogFile(sErrDesc, sFuncName)
            If p_iDebugMode = DEBUG_ON Then Call WriteToLogFile_Debug("Completed with ERROR", sFuncName)
            CreateCollectionEntry_baCKUP = RTN_ERROR
        End Try
    End Function

End Module
