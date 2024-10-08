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
        if ($null -ne $line) {
            return $line
        } else {
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
    $client.Connect($ipEndPoint)
    
    $requestObject = [PSCustomObject]@{
        RequestType = 0;
        RequestBody = $command
    }

    Write-Host -ForegroundColor Green "Sending command: $command"

    $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress
    Send-Message -client $client -message $jsonMessage
    $response = Receive-Message -client $client

    Write-Host -ForegroundColor Green "Response received: $response"
    
    $responseObject = ConvertFrom-Json -InputObject $response
    Write-Output $responseObject
    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}

# 1. Crear una tabla
Send-SQLCommand -command "CREATE TABLE ESTUDIANTES"

# 2. Insertar registros en la tabla
Send-SQLCommand -command "INSERT INTO ESTUDIANTES VALUES (1, 'Isaac', 'Ramirez')"
Send-SQLCommand -command "INSERT INTO ESTUDIANTES VALUES (2, 'Maria', 'Gonzalez')"

# 3. Seleccionar registros de la tabla
Send-SQLCommand -command "SELECT * FROM ESTUDIANTES"

# 4. Actualizar un registro
Send-SQLCommand -command "UPDATE ESTUDIANTES SET (1, 'Isaac', 'Lopez')"

# 5. Seleccionar registros de nuevo para verificar la actualización
Send-SQLCommand -command "SELECT * FROM ESTUDIANTES"

# 6. Eliminar un registro
Send-SQLCommand -command "DELETE FROM ESTUDIANTES WHERE id = 2"

# 7. Seleccionar registros de nuevo para verificar la eliminación
Send-SQLCommand -command "SELECT * FROM ESTUDIANTES"
