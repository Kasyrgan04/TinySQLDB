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
        $writer.Flush()  # Asegúrate de que el mensaje se envíe
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
            Write-Host "No se recibió ninguna línea."
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
        Write-Host -ForegroundColor Red "No se recibió respuesta del servidor."
        return
    }

    $responseObject = ConvertFrom-Json -InputObject $response

    # Cambiar el color de la salida según el valor de Status
    switch ($responseObject.status) {
        0 { $color = "Green" }
        1 { $color = "Red" }
        2 { $color = "Yellow" }
        default { $color = "Green" } 
    }

    # Mostrar un mensaje breve de respuesta
    Write-Host -ForegroundColor $color "Respuesta recibida con estado: $($responseObject.status)"

    # Procesar datos en la respuesta
    if ($responseObject.responseData -ne $null) {
        $columns = $responseObject.responseData.columns
        $rows = $responseObject.responseData.rows

        # Convertir las filas a objetos de PowerShell
        $data = foreach ($row in $rows) {
            $obj = New-Object PSObject
            foreach ($column in $columns) {
                $value = $row.$column

                # Convertir fechas de cadena a objetos DateTime
                if ($value -is [string] -and $value -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') {
                    $value = [DateTime]$value
                }

                # Convertir números a cadenas para alinear a la izquierda
                if ($value -is [int] -or $value -is [double]) {
                    $value = $value.ToString()
                }

                $obj | Add-Member -MemberType NoteProperty -Name $column -Value $value
            }
            $obj
        }

        # Mostrar los datos en formato de tabla
        $data | Format-Table -AutoSize
    } else {
        Write-Host -ForegroundColor $color $responseObject.responseBody
    }

    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}

# Bucle para aceptar comandos SQL
while ($true) {
    $sqlCommand = Read-Host "Ingrese su comando SQL (o escriba 'salir' para terminar)"
    
    if ($sqlCommand -eq "salir") {
        break
    }

    Send-SQLCommand -command $sqlCommand
}
