import React, { useState, useCallback } from 'react'
import { searchNodes, fetchNodeDetail, expandNode } from './api'

export default function Sidebar({ overview, selectedNode, onNodeSelect, onExpandGraph, nodeColors }) {
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [nodeDetail, setNodeDetail] = useState(null)
  const [searchTimeout, setSearchTimeout] = useState(null)

  const handleSearch = useCallback((query) => {
    setSearchQuery(query)
    if (searchTimeout) clearTimeout(searchTimeout)
    if (query.length < 2) {
      setSearchResults([])
      return
    }
    const timeout = setTimeout(async () => {
      try {
        const data = await searchNodes(query)
        setSearchResults(data.results || [])
      } catch (err) {
        console.error('Search failed:', err)
      }
    }, 300)
    setSearchTimeout(timeout)
  }, [searchTimeout])

  const handleSelectSearchResult = async (result) => {
    setSearchResults([])
    setSearchQuery('')
    try {
      const detail = await fetchNodeDetail(result.label, result.id)
      setNodeDetail(detail)
      onNodeSelect({ id: result.id, label: result.label, properties: result.properties })

      const expanded = await expandNode(result.label, result.id)
      onExpandGraph(expanded.nodes, expanded.edges)
    } catch (err) {
      console.error('Node detail failed:', err)
    }
  }

  const handleNeighborClick = async (neighbor) => {
    try {
      const detail = await fetchNodeDetail(neighbor.label, neighbor.id)
      setNodeDetail(detail)
      onNodeSelect({ id: neighbor.id, label: neighbor.label, properties: neighbor.properties })

      const expanded = await expandNode(neighbor.label, neighbor.id)
      onExpandGraph(expanded.nodes, expanded.edges)
    } catch (err) {
      console.error('Neighbor expand failed:', err)
    }
  }

  React.useEffect(() => {
    if (selectedNode) {
      fetchNodeDetail(selectedNode.label, selectedNode.id)
        .then(setNodeDetail)
        .catch(console.error)
    }
  }, [selectedNode])

  const getNodeColor = (label) => nodeColors[label] || '#adb5bd'

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h1>SAP O2C Graph</h1>
        <p>Order-to-Cash Flow Explorer</p>
      </div>

      {/* Stats */}
      {overview && (
        <div className="stats-section">
          <div className="stats-grid">
            {overview.nodeStats?.slice(0, 6).map(s => (
              <div className="stat-card" key={s.label}>
                <div className="count">{s.count?.toLocaleString()}</div>
                <div className="label">{s.label}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Search */}
      <div className="search-section">
        <div className="search-input-wrapper">
          <input
            className="search-input"
            type="text"
            placeholder="Search nodes by ID or name..."
            value={searchQuery}
            onChange={e => handleSearch(e.target.value)}
          />
        </div>
        {searchResults.length > 0 && (
          <div className="search-results">
            {searchResults.map((r, i) => (
              <div
                key={i}
                className="search-result-item"
                onClick={() => handleSelectSearchResult(r)}
              >
                <span
                  className="node-badge"
                  style={{ background: getNodeColor(r.label), color: '#000' }}
                >
                  {r.label}
                </span>
                <span>{r.id}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Node Detail */}
      {nodeDetail && (
        <div className="node-detail">
          <div className="node-detail-header">
            <span
              className="node-badge"
              style={{ background: getNodeColor(nodeDetail.node?.label), color: '#000' }}
            >
              {nodeDetail.node?.label}
            </span>
            <h3>{nodeDetail.node?.id}</h3>
          </div>

          <table className="properties-table">
            <tbody>
              {nodeDetail.node?.properties && Object.entries(nodeDetail.node.properties)
                .filter(([k]) => k !== 'id')
                .map(([key, val]) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{val === null || val === '' ? '—' : String(val)}</td>
                  </tr>
                ))}
            </tbody>
          </table>

          {nodeDetail.neighbors && nodeDetail.neighbors.length > 0 && (
            <div className="neighbors-section">
              <h4>Connected Nodes ({nodeDetail.neighbors.length})</h4>
              {nodeDetail.neighbors.slice(0, 30).map((n, i) => (
                <div key={i} className="neighbor-item" onClick={() => handleNeighborClick(n)}>
                  <span
                    className="node-badge"
                    style={{ background: getNodeColor(n.label), color: '#000', fontSize: 9 }}
                  >
                    {n.label}
                  </span>
                  <span>{n.id}</span>
                  <span className="rel-type">{n.direction === 'outgoing' ? '→' : '←'} {n.relType}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {!nodeDetail && !searchResults.length && (
        <div style={{ padding: '20px 24px', color: '#555', fontSize: 12, lineHeight: 1.6 }}>
          <p>Click a node in the 3D graph or search above to inspect it.</p>
          <p style={{ marginTop: 10, color: '#444' }}>
            Hover over nodes to see details. Click to expand and traverse the graph.
          </p>
        </div>
      )}
    </div>
  )
}
