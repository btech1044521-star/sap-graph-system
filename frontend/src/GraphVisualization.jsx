import React, { useRef, useCallback, useEffect, useState, useMemo } from 'react'
import ForceGraph3D from 'react-force-graph-3d'
import SpriteText from 'three-spritetext'
import * as THREE from 'three'
import { expandNode } from './api'

const NODE_SHAPES = {
  Customer: 'sphere',
  SalesOrder: 'box',
  SalesOrderItem: 'box',
  Delivery: 'octahedron',
  DeliveryItem: 'octahedron',
  BillingDocument: 'dodecahedron',
  BillingDocumentItem: 'dodecahedron',
  JournalEntry: 'torus',
  Payment: 'cone',
  Product: 'icosahedron',
  Plant: 'cylinder',
  Address: 'tetrahedron',
}

export default function GraphVisualization({ graphData, onNodeSelect, onExpandGraph, nodeColors, highlightNodes }) {
  const graphRef = useRef()
  const containerRef = useRef()
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 })
  const [hoverNode, setHoverNode] = useState(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })

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

  const formattedData = useMemo(() => {
    const nodes = graphData.nodes.map(n => ({
      id: n.id,
      label: n.label,
      name: n.properties?.name || n.properties?.shortName || n.properties?.description || n.id,
      properties: n.properties,
      color: nodeColors[n.label] || '#888',
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
    // Focus camera on clicked node
    const distance = 120
    if (graphRef.current) {
      const { x, y, z } = node
      graphRef.current.cameraPosition(
        { x: x + distance, y: y + distance / 2, z: z + distance },
        { x, y, z },
        1000
      )
    }
    try {
      const data = await expandNode(node.label, node.id)
      onExpandGraph(data.nodes, data.edges)
    } catch (err) {
      console.error('Expand failed:', err)
    }
  }, [onNodeSelect, onExpandGraph])

  const handleNodeHover = useCallback((node, prevNode) => {
    setHoverNode(node || null)
    if (containerRef.current) {
      containerRef.current.style.cursor = node ? 'pointer' : 'default'
    }
  }, [])

  const handlePointerMove = useCallback((e) => {
    if (containerRef.current) {
      const rect = containerRef.current.getBoundingClientRect()
      setTooltipPos({ x: e.clientX - rect.left + 14, y: e.clientY - rect.top + 14 })
    }
  }, [])

  const createNodeObject = useCallback((node) => {
    const isHighlighted = highlightNodes.size > 0 && highlightNodes.has(node.id)
    const isDimmed = highlightNodes.size > 0 && !highlightNodes.has(node.id)
    const shape = NODE_SHAPES[node.label] || 'sphere'
    const baseSize = isHighlighted ? 6 : 4
    const color = new THREE.Color(node.color)
    if (isDimmed) color.multiplyScalar(0.25)

    const mat = new THREE.MeshPhongMaterial({
      color,
      transparent: isDimmed,
      opacity: isDimmed ? 0.15 : 0.95,
      shininess: 80,
    })

    let geometry
    switch (shape) {
      case 'box':
        geometry = new THREE.BoxGeometry(baseSize, baseSize, baseSize)
        break
      case 'octahedron':
        geometry = new THREE.OctahedronGeometry(baseSize * 0.7)
        break
      case 'dodecahedron':
        geometry = new THREE.DodecahedronGeometry(baseSize * 0.7)
        break
      case 'torus':
        geometry = new THREE.TorusGeometry(baseSize * 0.5, baseSize * 0.2, 8, 16)
        break
      case 'cone':
        geometry = new THREE.ConeGeometry(baseSize * 0.5, baseSize, 8)
        break
      case 'icosahedron':
        geometry = new THREE.IcosahedronGeometry(baseSize * 0.7)
        break
      case 'cylinder':
        geometry = new THREE.CylinderGeometry(baseSize * 0.4, baseSize * 0.4, baseSize, 12)
        break
      case 'tetrahedron':
        geometry = new THREE.TetrahedronGeometry(baseSize * 0.7)
        break
      default:
        geometry = new THREE.SphereGeometry(baseSize * 0.6, 16, 12)
    }

    const mesh = new THREE.Mesh(geometry, mat)

    // Glow ring for highlighted nodes
    if (isHighlighted) {
      const ring = new THREE.Mesh(
        new THREE.RingGeometry(baseSize * 0.8, baseSize * 1.1, 32),
        new THREE.MeshBasicMaterial({ color: 0x818cf8, transparent: true, opacity: 0.7, side: THREE.DoubleSide })
      )
      mesh.add(ring)
    }

    // Label sprite
    const displayName = (node.name || node.id).substring(0, 18)
    const sprite = new SpriteText(displayName)
    sprite.color = isDimmed ? 'rgba(200,200,255,0.1)' : 'rgba(220,220,255,0.9)'
    sprite.textHeight = 2.5
    sprite.position.y = -(baseSize + 2)
    sprite.fontFace = 'Inter, Segoe UI, sans-serif'
    sprite.fontWeight = '500'
    mesh.add(sprite)

    return mesh
  }, [highlightNodes])

  const handleZoomToFit = () => {
    if (graphRef.current) graphRef.current.zoomToFit(400, 80)
  }

  const handleResetCamera = () => {
    if (graphRef.current) {
      graphRef.current.cameraPosition({ x: 0, y: 0, z: 300 }, { x: 0, y: 0, z: 0 }, 1000)
    }
  }

  // Tooltip content
  const tooltipContent = useMemo(() => {
    if (!hoverNode) return null
    const props = hoverNode.properties || {}
    const entries = Object.entries(props).filter(([k]) => k !== 'id').slice(0, 8)
    return { label: hoverNode.label, id: hoverNode.id, name: hoverNode.name, entries }
  }, [hoverNode])

  return (
    <div className="graph-container" ref={containerRef} onPointerMove={handlePointerMove}>
      {/* Legend */}
      <div className="graph-legend">
        {Object.entries(nodeColors).map(([label, color]) => (
          <div className="legend-item" key={label}>
            <div className="legend-dot" style={{ background: color }} />
            <span>{label}</span>
          </div>
        ))}
      </div>

      {/* Controls */}
      <div className="graph-controls">
        <button onClick={handleZoomToFit}>Fit View</button>
        <button onClick={handleResetCamera}>Reset</button>
      </div>

      {/* Hover Tooltip */}
      {tooltipContent && (
        <div className="graph-tooltip" style={{ left: tooltipPos.x, top: tooltipPos.y }}>
          <div className="tooltip-header">
            <span className="tooltip-badge">{tooltipContent.label}</span>
            <span className="tooltip-id">{tooltipContent.id}</span>
          </div>
          {tooltipContent.name !== tooltipContent.id && (
            <div className="tooltip-name">{tooltipContent.name}</div>
          )}
          {tooltipContent.entries.length > 0 && (
            <table className="tooltip-props">
              <tbody>
                {tooltipContent.entries.map(([k, v]) => (
                  <tr key={k}>
                    <td>{k}</td>
                    <td>{v === null || v === '' ? '—' : String(v).substring(0, 40)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <div className="tooltip-hint">Click to expand</div>
        </div>
      )}

      {/* 3D Graph */}
      <ForceGraph3D
        ref={graphRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={formattedData}
        nodeThreeObject={createNodeObject}
        nodeThreeObjectExtend={false}
        onNodeClick={handleNodeClick}
        onNodeHover={handleNodeHover}
        linkColor={() => 'rgba(120,140,255,0.25)'}
        linkWidth={0.5}
        linkOpacity={0.35}
        linkDirectionalArrowLength={3}
        linkDirectionalArrowRelPos={1}
        linkDirectionalArrowColor={() => 'rgba(100,120,255,0.5)'}
        linkDirectionalParticles={3}
        linkDirectionalParticleWidth={1.8}
        linkDirectionalParticleSpeed={0.006}
        linkDirectionalParticleColor={() => '#818cf8'}
        cooldownTicks={80}
        d3AlphaDecay={0.04}
        d3VelocityDecay={0.3}
        backgroundColor="#0c0c1a"
        showNavInfo={false}
        enableNavigationControls={true}
      />
    </div>
  )
}
