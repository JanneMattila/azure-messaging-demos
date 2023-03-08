$eventHubNamespace = "<your event hub namespace name>"
$eventHubInstance = "<your event hub instance name>"
$tenantId = "<your tenant id>"

$url = "https://$eventHubNamespace.servicebus.windows.net/$eventHubInstance/messages?api-version=2014-01"
$url

Login-AzAccount
Login-AzAccount -Tenant $tenantId

$accessToken = Get-AzAccessToken -ResourceUrl https://eventhubs.azure.net
$accessToken.Token

$accessToken = ConvertTo-SecureString -AsPlainText -String $accessToken.Token
$body = ConvertTo-Json @{
    "specversion"     = "1.0"
    "type"            = "ERP.Sales.Order.Created"
    "source"          = "/mycontext"
    "subject"         = $null
    "id"              = "C1234-1234-1234"
    "time"            = [System.DateTime]::UtcNow
    "datacontenttype" = "application/json"
    "data"            = @{
        "appinfoA" = "abc"
        "appinfoB" = 123
        "appinfoC" = $true
    }
}
$body

Invoke-RestMethod `
    -Body $body `
    -ContentType "application/atom+xml;type=entry;charset=utf-8" `
    -Method "POST" `
    -Authentication Bearer `
    -Token $accessToken `
    -Uri $url
