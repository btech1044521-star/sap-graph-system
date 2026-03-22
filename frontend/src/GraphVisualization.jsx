import React, { useRef, useCallback, useEffect, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { expandNode } from './api'

export default function GraphVisualization({ graphData, onNodeSelect, onExpandGraph, nodeColors, highlightNodes }) {
  const graphRef = useRef()
  const containerRef = useRef()
  const [dimensions, setDimensions] = useState({ width: 800, height: 400 })

  useEffect(() => {
    const updateDimensions = () => {
      if (containerRef.current) {
        setDimensions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight
        })
      }
    }
    updateDimensions()
    window.addEventListener('resize', updateDimensions)
    const observer = new ResizeObserver(updateDimensions)
    if (containerRef.current) observer.observe(containerRef.current)
    return () => {
      window.removeEventListener('resize', updateDimensions)
      observer.disconnect()
    }
  }, [])

  const formattedData = React.useMemo(() => {
    const nodes = graphData.nodes.map(n => ({
      id: n.id,
      label: n.label,
      name: n.properties?.name || n.properties?.shortName || n.properties?.description || n.id,
      properties: n.properties,
      color: nodeColors[n.label] || '#adb5bd',
    }))
    const nodeIds = new Set(nodes.map(n => n.id))
    const links = graphData.edges
      .filter(e => {
        const src = typeof e.source === 'object' ? e.source.id : e.source
        const tgt = typeof e.target === 'object' ? e.target.id : e.target
        return nodeIds.has(src) && nodeIds.has(tgt)
      })
      .map(e => ({
        source: typeof e.source === 'object' ? e.source.id : e.source,
        target: typeof e.target === 'object' ? e.target.id : e.target,
        type: e.type
      }))
    return { nodes, links }
  }, [graphData, nodeColors])

  const handleNodeClick = useCallback(async (node) => {
    onNodeSelect({ id: node.id, label: node.label, properties: node.properties })

    try {
      const data = await expandNode(node.label, node.id)
      onExpandGraph(data.nodes, data.edges)
    } catch (err) {
      console.error('Expand failed:', err)
    }
  }, [onNodeSelect, onExpandGraph])

  const paintNode = useCallback((node, ctx, globalScale) => {
    const fontSize = Math.max(12 / globalScale, 3)
    const radius = highlightNodes.has(node.id) ? 8 : 5
    const alpha = highlightNodes.size > 0 ? (highlightNodes.has(node.id) ? 1 : 0.2) : 1

    ctx.globalAlpha = alpha

    // Node circle
    ctx.beginPath()
    ctx.arc(node.x, node.y, radius, 0, 2 * Math.PI)
    ctx.fillStyle = node.color
    ctx.fill()

    if (highlightNodes.has(node.id)) {
      ctx.strokeStyle = '#fff'
      ctx.lineWidth = 2 / globalScale
      ctx.stroke()
    }

    // Label
    if (globalScale > 1.2 || highlightNodes.has(node.id)) {
      ctx.font = `${fontSize}px 'Segoe UI', sans-serif`
      ctx.textAlign = 'center'
      ctx.textBaseline = 'top'
      ctx.fillStyle = '#e4e6eb'
      const displayName = (node.name || node.id).substring(0, 20)
      ctx.fillText(displayName, node.x, node.y + radius + 2)
    }

    ctx.globalAlpha = 1
  }, [highlightNodes])

  const paintLink = useCallback((link, ctx, globalScale) => {
    const alpha = highlightNodes.size > 0
      ? (highlightNodes.has(link.source.id) || highlightNodes.has(link.target.id) ? 0.6 : 0.05)
      : 0.15
    ctx.globalAlpha = alpha
    ctx.strokeStyle = '#4f8cff'
    ctx.lineWidth = 0.5
    ctx.beginPath()
    ctx.moveTo(link.source.x, link.source.y)
    ctx.lineTo(link.target.x, link.target.y)
    ctx.stroke()

    // Edge label when zoomed in
    if (globalScale > 2.5) {
      const midX = (link.source.x + link.target.x) / 2
      const midY = (link.source.y + link.target.y) / 2
      ctx.font = `${8 / globalScale}px 'Segoe UI', sans-serif`
      ctx.textAlign = 'center'
      ctx.fillStyle = '#a0a3ab'
      ctx.fillText(link.type, midX, midY)
    }

    ctx.globalAlpha = 1
  }, [highlightNodes])

  const handleZoomToFit = () => {
    if (graphRef.current) graphRef.current.zoomToFit(400, 50)
  }

  return (
    <div className="graph-container" ref={containerRef}>
      <div className="graph-legend">
        {Object.entries(nodeColors).map(([label, color]) => (
          <div className="legend-item" key={label}>
            <div className="legend-dot" style={{ background: color }} />
            <span>{label}</span>
          </div>
        ))}
      </div>
      <div className="graph-controls">
        <button onClick={handleZoomToFit}>Fit View</button>
        <button onClick={() => graphRef.current?.centerAt(0, 0, 400)}>Center</button>
      </div>
      <ForceGraph2D
        ref={graphRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={formattedData}
        nodeCanvasObject={paintNode}
        linkCanvasObject={paintLink}
        onNodeClick={handleNodeClick}
        nodePointerAreaPaint={(node, color, ctx) => {
          ctx.beginPath()
          ctx.arc(node.x, node.y, 8, 0, 2 * Math.PI)
          ctx.fillStyle = color
          ctx.fill()
        }}
        cooldownTicks={100}
        d3AlphaDecay={0.05}
        d3VelocityDecay={0.3}
        backgroundColor="#0f1117"
        enableZoomInteraction={true}
        enablePanInteraction={true}
      />
    </div>
  )
}
