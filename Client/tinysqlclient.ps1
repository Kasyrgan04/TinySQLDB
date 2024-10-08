param (
    [Parameter(Mandatory = $true)]
    [string]$IP,
    
    [Parameter(Mandatory = $true)]
    [int]$Port
)

$ipEndPoint = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Parse($IP), $Port)

function Send-Message {
    param (
        [Parameter(Mandatory=$true)]
        [pscustomobject]$message,
        
        [Parameter(Mandatory=$true)]
        [System.Net.Sockets.Socket]$client
    )

    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $writer = New-Object System.IO.StreamWriter($stream)

    try {
        $writer.WriteLine($message)
        $writer.Flush()  
    }
    finally {
        $writer.Close()
        $stream.Close()
    }
}

function Receive-Message {
    param (
        [System.Net.Sockets.Socket]$client
    )
    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $reader = New-Object System.IO.StreamReader($stream)

    try {
        $line = $reader.ReadLine()
        
        if ($line -ne $null) {
            return $line
        } else {
            Write-Host "No se recibi� ninguna l�nea."
            return ""
        }
    }
    finally {
        $reader.Close()
        $stream.Close()
    }
}

function Send-SQLCommand {
    param (
        [string]$command
    )

    $client = New-Object System.Net.Sockets.Socket($ipEndPoint.AddressFamily, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)

    try {
        $client.Connect($ipEndPoint)
    } catch {
        Write-Host -ForegroundColor Red "Error al conectar con el servidor: $_"
        return
    }
    
    $requestObject = [PSCustomObject]@{
        RequestType = 0;
        RequestBody  = $command
    }

    Write-Host -ForegroundColor Green "Enviando comando: $command"

    $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress
    Send-Message -client $client -message $jsonMessage
    $response = Receive-Message -client $client

    if ([string]::IsNullOrEmpty($response)) {
        Write-Host -ForegroundColor Red "No se recibi� respuesta del servidor."
        return
    }

    $responseObject = ConvertFrom-Json -InputObject $response

    
    switch ($responseObject.status) {
        0 { $color = "Green" }
        1 { $color = "Red" }
        2 { $color = "Yellow" }
        default { $color = "Green" } 
    }

    
    Write-Host -ForegroundColor $color "Respuesta recibida"

    
    if ($responseObject.responseData -ne $null) {
        $columns = $responseObject.responseData.columns
        $rows = $responseObject.responseData.rows

        
        $data = foreach ($row in $rows) {
            $obj = New-Object PSObject
            foreach ($column in $columns) {
                $value = $row.$column

                
                if ($value -is [string] -and $value -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') {
                    $value = [DateTime]$value
                }

                
                if ($value -is [int] -or $value -is [double]) {
                    $value = $value.ToString()
                }

                $obj | Add-Member -MemberType NoteProperty -Name $column -Value $value
            }
            $obj
        }

        
        $data | Format-Table -AutoSize
    } else {
        Write-Host -ForegroundColor $color $responseObject.responseBody
    }

    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}


while ($true) {
    $sqlCommand = Read-Host "Esperando comando. Para cerrar la sesi�n ingrese EXIT"
    
    if ($sqlCommand -eq "EXIT") {
        break
    }

    Send-SQLCommand -command $sqlCommand
}


