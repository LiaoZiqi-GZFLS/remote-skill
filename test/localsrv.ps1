# Tiny local HTTP service used by the reverse forward test.
# Listens on 127.0.0.1:$Port and answers $Requests requests, then exits.
param([int]$Port = 19090, [int]$Requests = 3)
$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("http://127.0.0.1:$Port/")
$listener.Start()
for ($i = 0; $i -lt $Requests; $i++) {
    $ctx = $listener.GetContext()
    $body = [Text.Encoding]::UTF8.GetBytes("local-service-ok")
    $ctx.Response.ContentType = "text/plain"
    $ctx.Response.OutputStream.Write($body, 0, $body.Length)
    $ctx.Response.Close()
}
$listener.Stop()
