VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Sheet1"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True
Private Sub data_types_Click()
    Dim ws As Worksheet
    Dim rng As Range
    Dim cell As Range
    Dim dbType As String
    
    ' Set worksheet and range
    Set ws = ThisWorkbook.Sheets("Schema") ' Change sheet name if necessary
    Set rng = ws.Range("C2:C200")
    
    ' Loop through each cell in range
    For Each cell In rng
        ' Check if the cell is empty, then skip processing
        If Trim(cell.Value) <> "" Then
            Select Case True
                ' Numeric types
                Case cell.Value Like "number*"
                    dbType = "bigint"
                Case cell.Value Like "int"
                    dbType = "int"
                Case cell.Value Like "double*"
                    dbType = "double"
                Case cell.Value Like "float*"
                    dbType = "float"
                Case cell.Value Like "decimal*"
                    dbType = "decimal"
                    
                ' String types
                Case cell.Value Like "varchar2(*)"
                    dbType = "string"
                Case cell.Value Like "char(*)"
                    dbType = "string"
                Case cell.Value Like "nvarchar2(*)"
                    dbType = "string"
                Case cell.Value Like "clob"
                    dbType = "string"
                    
                ' Date and Time types
                Case cell.Value Like "timestamp"
                    dbType = "timestamp"
                Case cell.Value Like "date"
                    dbType = "date"
                
                ' Boolean and Miscellaneous types
                Case cell.Value Like "boolean"
                    dbType = "boolean"
                Case cell.Value Like "blob"
                    dbType = "binary"
                Case cell.Value Like "raw(*)"
                    dbType = "binary"
                    
                ' Default case
                Case Else
                    dbType = "unknown" ' Fallback for unrecognized types
            End Select
            
            ' Write to column D
            cell.Offset(0, 1).Value = dbType
        End If
    Next cell
End Sub

Private Sub lowerCase_Click()
    Dim ws As Worksheet
    Dim rng As Range
    Dim cell As Range
    
    ' Set the worksheet to "schema"
    Set ws = ThisWorkbook.Sheets("schema")
    
    ' Convert text to lowercase in B2:B200 and C2:C200
    Set rng = ws.Range("B2:B200, C2:C200")
    
    For Each cell In rng
        If Not IsEmpty(cell.Value) Then cell.Value = LCase(cell.Value)
    Next cell
    
    MsgBox "Text converted to lowercase in 'schema' sheet!", vbInformation, "Done"
End Sub


Private Sub Reformat_Click()
    Dim ws As Worksheet
    Dim rowNum As Long
    
    ' Set the worksheet to "schema"
    Set ws = ThisWorkbook.Sheets("schema")
    
    ' Adjust row height if greater than 25 pixels
    For rowNum = 2 To 200
        If ws.Rows(rowNum).RowHeight > 25 Then ws.Rows(rowNum).RowHeight = 15
    Next rowNum
    
    MsgBox "Row heights adjusted in 'schema' sheet!", vbInformation, "Done"
End Sub

Private Sub removeSpace_comma_Click()
    Dim ws As Worksheet
    Dim rng As Range
    Dim cell As Range
    Dim cleanedValue As String
    
    ' Set worksheet and range for column B
    Set ws = ThisWorkbook.Sheets("schema") ' Change sheet name if necessary
    Set rng = ws.Range("B2:B200")
    
    ' Loop through each cell in range B2:B200
    For Each cell In rng
        If Trim(cell.Value) <> "" Then
            cleanedValue = Replace(cell.Value, " ", "") ' Remove spaces
            cleanedValue = Replace(cleanedValue, ",", "") ' Remove commas
            cell.Value = cleanedValue
        End If
    Next cell
    
    ' Set range for column C
    Set rng = ws.Range("C2:C200")
    
    ' Loop through each cell in range C2:C200
    For Each cell In rng
        If Trim(cell.Value) <> "" Then
            cleanedValue = Replace(cell.Value, " ", "") ' Remove spaces
            cleanedValue = Replace(cleanedValue, ",", "") ' Remove commas
            cell.Value = cleanedValue
        End If
    Next cell
End Sub
