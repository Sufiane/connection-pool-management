const pooledDownload = async (connect, save, downloadList, maxConcurrency) => {
    const connectionsPool = await getConnectionsPool(connect, maxConcurrency)

    if (connectionsPool.length === 0) {
        throw new Error("connection failed")
    }

    const maxFilePerConnection = downloadList.length / connectionsPool.length

    await Promise.all(connectionsPool.map((_, i) => {
        const {start, end} = i === 0 ? {start: 0, end: maxFilePerConnection} : {
            start: i * maxFilePerConnection,
            end: (i + 1) * maxFilePerConnection
        }

        return downloadFiles(connectionsPool[i], save, downloadList.slice(start, end))
    })).catch((e) => {
        connectionsPool.map((_, i) => connectionsPool[i].close())

        throw e
    })
}

const getConnections = async (connect) => {
    return connect().catch(() => {
        throw new Error("connection failed")
    })
}

const downloadFiles = async ({download, close}, save, filesToDownload) => {
    for await (const file of filesToDownload) {
        save(await download(file).catch(e => {
            throw e
        }))
    }

    close()

}

const getConnectionsPool = async (connect, maxConcurrency) => {
    const connections = []

    for (let i = 0; i < maxConcurrency; i++) {
        const connection = await getConnections(connect).catch(() => undefined)

        if (!connection) {
            break;
        }

        connections.push(connection)
    }

    return connections
}


module.exports = pooledDownload
