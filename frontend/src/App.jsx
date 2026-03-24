import React, { useState, useEffect, useCallback } from 'react'
import GraphVisualization from './GraphVisualization'
import ChatPanel from './ChatPanel'
import Sidebar from './Sidebar'
import { fetchGraphOverview, fetchGraphSample } from './api'

const NODE_COLORS = {
  Customer: '#6366f1',
  SalesOrder: '#22d3ee',
  SalesOrderItem: '#38bdf8',
  Delivery: '#f97316',
  DeliveryItem: '#fb923c',
  BillingDocument: '#a78bfa',
  BillingDocumentItem: '#c084fc',
  JournalEntry: '#34d399',
  Payment: '#facc15',
  Product: '#f472b6',
  Plant: '#4ade80',
  Address: '#94a3b8',
}

export default function App() {
  const [graphData, setGraphData] = useState({ nodes: [], edges: [] })
  const [overview, setOverview] = useState(null)
  const [selectedNode, setSelectedNode] = useState(null)
  const [highlightNodes, setHighlightNodes] = useState(new Set())

  useEffect(() => {
    fetchGraphOverview().then(setOverview).catch(console.error)
    fetchGraphSample(60).then(data => {
      setGraphData(data)
    }).catch(console.error)
  }, [])

  const handleNodeSelect = useCallback((node) => {
    setSelectedNode(node)
  }, [])

  const handleExpandGraph = useCallback((newNodes, newEdges) => {
    setGraphData(prev => {
      const existingNodeIds = new Set(prev.nodes.map(n => n.id))
      const existingEdgeKeys = new Set(prev.edges.map(e => `${e.source}-${e.target}-${e.type}`))
      const addedNodes = newNodes.filter(n => !existingNodeIds.has(n.id))
      const addedEdges = newEdges.filter(e => !existingEdgeKeys.has(`${e.source}-${e.target}-${e.type}`))
      return {
        nodes: [...prev.nodes, ...addedNodes],
        edges: [...prev.edges, ...addedEdges]
      }
    })
  }, [])

  const handleHighlightNodes = useCallback((nodeIds) => {
    setHighlightNodes(new Set(nodeIds))
  }, [])

  return (
    <div className="app-container">
      <Sidebar
        overview={overview}
        selectedNode={selectedNode}
        onNodeSelect={handleNodeSelect}
        onExpandGraph={handleExpandGraph}
        nodeColors={NODE_COLORS}
      />
      <div className="main-area">
        <GraphVisualization
          graphData={graphData}
          onNodeSelect={handleNodeSelect}
          onExpandGraph={handleExpandGraph}
          nodeColors={NODE_COLORS}
          highlightNodes={highlightNodes}
        />
        <ChatPanel
          onHighlightNodes={handleHighlightNodes}
        />
      </div>
    </div>
  )
}
