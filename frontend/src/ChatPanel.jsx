import React, { useState, useRef, useEffect } from 'react'
import { sendChatMessage } from './api'

const EXAMPLE_QUERIES = [
  "Which products are associated with the highest number of billing documents?",
  "Trace the full flow of billing document 90504248",
  "Find sales orders that are delivered but not billed",
  "What is the total billing amount by customer?",
  "Which plants handle the most deliveries?",
  "Show me incomplete order flows",
]

export default function ChatPanel({ onHighlightNodes }) {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: 'Welcome! I can help you explore the SAP Order-to-Cash data. Ask me about sales orders, deliveries, billing documents, payments, customers, products, and their relationships.\n\nTry one of the example queries below, or ask your own question.',
    }
  ])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [showExamples, setShowExamples] = useState(true)
  const messagesEndRef = useRef(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const handleSend = async (text = null) => {
    const messageText = text || input.trim()
    if (!messageText || loading) return

    setShowExamples(false)
    setInput('')
    const userMsg = { role: 'user', content: messageText }
    setMessages(prev => [...prev, userMsg])
    setLoading(true)

    try {
      const history = messages
        .filter(m => m.role !== 'system')
        .slice(-6)
        .map(m => ({ role: m.role, content: m.content }))

      const response = await sendChatMessage(messageText, history)

      const assistantMsg = {
        role: 'assistant',
        content: response.answer,
        cypher: response.cypher,
        guardrail: response.guardrail,
        results: response.results,
      }
      setMessages(prev => [...prev, assistantMsg])

      // Highlight nodes referenced in results
      if (response.results && response.results.length > 0 && onHighlightNodes) {
        const nodeIds = new Set()
        for (const row of response.results) {
          for (const val of Object.values(row)) {
            if (typeof val === 'string' && val.length > 2 && val.length < 30) {
              nodeIds.add(val)
            }
          }
        }
        onHighlightNodes([...nodeIds])
        // Clear highlight after 10 seconds
        setTimeout(() => onHighlightNodes([]), 10000)
      }
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Sorry, there was an error processing your request. Please try again.',
      }])
    } finally {
      setLoading(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <h3>Query Assistant</h3>
        <span style={{ fontSize: 10, color: '#555', letterSpacing: '0.5px', textTransform: 'uppercase' }}>
          Gemini + Neo4j
        </span>
      </div>
      <div className="chat-messages">
        {messages.map((msg, i) => (
          <div key={i} className={`chat-message ${msg.role} ${msg.guardrail ? 'guardrail' : ''}`}>
            <div>{msg.content}</div>
            {msg.cypher && (
              <div className="cypher-block">
                {msg.cypher}
              </div>
            )}
          </div>
        ))}
        {showExamples && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 4 }}>
            {EXAMPLE_QUERIES.map((q, i) => (
              <button
                key={i}
                onClick={() => handleSend(q)}
                style={{
                  padding: '6px 12px',
                  background: '#111',
                  border: '1px solid #222',
                  borderRadius: 4,
                  color: '#aaa',
                  fontSize: 11,
                  cursor: 'pointer',
                  textAlign: 'left',
                  transition: 'border-color 0.15s',
                }}
                onMouseEnter={e => e.target.style.borderColor = '#444'}
                onMouseLeave={e => e.target.style.borderColor = '#222'}
              >
                {q}
              </button>
            ))}
          </div>
        )}
        {loading && (
          <div className="chat-message assistant">
            <div className="loading-dots">
              <span /><span /><span />
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="chat-input-area">
        <input
          className="chat-input"
          type="text"
          placeholder="Ask about O2C data... (e.g., 'Which products have the most billing documents?')"
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={loading}
        />
        <button className="send-btn" onClick={() => handleSend()} disabled={loading || !input.trim()}>
          Send
        </button>
      </div>
    </div>
  )
}
