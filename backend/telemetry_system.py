"""
Query Engine Telemetry & Testing System

Measures performance metrics including:
- Success rate
- Error classification
- Retry effectiveness
- Response times
- Strategy effectiveness
- Synthetic test case generation

This module works with llm_engine.py for actual query execution.
"""

import time
import json
import logging
import asyncio
import random
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from collections import defaultdict
import statistics
from pathlib import Path

# Import your actual query engine
try:
    from llm_engine import query, get_metrics
    QUERY_ENGINE_AVAILABLE = True
    print("✅ Connected to llm_engine.py")
except ImportError as e:
    QUERY_ENGINE_AVAILABLE = False
    print(f"⚠️  Query engine not available: {e}")
    
    # Mock query function for testing
    def query(user_query: str, conversation_history: list = None) -> dict:
        return {
            "answer": "Mock response",
            "cypher": "MATCH (n) RETURN n LIMIT 10",
            "results": [],
            "guardrail": False,
            "error": False,
            "strategy_used": "mock",
            "attempts": 1
        }
    
    def get_metrics() -> dict:
        return {"total_queries": 0, "successful": 0, "failed": 0}

try:
    from database import run_cypher
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("⚠️  Database not available. Using mock data.")
    
    def run_cypher(query: str, timeout: int = 30) -> list:
        return []

logger = logging.getLogger(__name__)


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class TestCase:
    """Test case for validation."""
    id: str
    natural_language: str
    expected_pattern: str = ""  # Regex or pattern to match in generated query
    expected_relationships: List[str] = field(default_factory=list)
    expected_node_labels: List[str] = field(default_factory=list)
    should_succeed: bool = True
    tags: List[str] = field(default_factory=list)
    expected_min_results: int = 0


@dataclass
class QueryTelemetry:
    """Single query telemetry record."""
    query_id: str
    timestamp: datetime
    user_query: str
    generated_cypher: str
    execution_time_ms: float
    success: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    attempts: int = 1
    strategy_used: str = "direct"
    guardrail_triggered: bool = False
    results_count: int = 0
    
    def to_dict(self) -> dict:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass
class TelemetrySummary:
    """Summary statistics for telemetry data."""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    guardrail_queries: int = 0
    error_distribution: Dict[str, int] = field(default_factory=dict)
    strategy_effectiveness: Dict[str, Dict[str, int]] = field(default_factory=dict)
    
    def update_from_telemetry(self, telemetry: QueryTelemetry):
        """Update summary with new telemetry record."""
        self.total_queries += 1
        
        if telemetry.success:
            self.successful_queries += 1
        elif telemetry.guardrail_triggered:
            self.guardrail_queries += 1
        else:
            self.failed_queries += 1
            if telemetry.error_type:
                self.error_distribution[telemetry.error_type] = \
                    self.error_distribution.get(telemetry.error_type, 0) + 1
        
        # Track strategy effectiveness
        if telemetry.strategy_used not in self.strategy_effectiveness:
            self.strategy_effectiveness[telemetry.strategy_used] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0
            }
        self.strategy_effectiveness[telemetry.strategy_used]["attempts"] += 1
        if telemetry.success:
            self.strategy_effectiveness[telemetry.strategy_used]["successes"] += 1
        else:
            self.strategy_effectiveness[telemetry.strategy_used]["failures"] += 1


# ============================================================================
# SYNTHETIC TEST CASE GENERATOR
# ============================================================================

class SyntheticTestCaseGenerator:
    """Generates synthetic test cases for comprehensive testing."""
    
    def __init__(self):
        # Common node types from your schema
        self.node_labels = [
            'Customer', 'SalesOrder', 'SalesOrderItem', 'BillingDocument',
            'BillingDocumentItem', 'Product', 'Delivery', 'DeliveryItem',
            'JournalEntry', 'Payment', 'Plant', 'Address'
        ]
        
        # Common relationship types
        self.relationships = [
            'PLACED_ORDER', 'HAS_ITEM', 'CONTAINS_PRODUCT', 'BILLED_TO',
            'BILLS_PRODUCT', 'FULFILLED_BY', 'PAID_BY', 'PAYS_FOR',
            'PRODUCED_AT', 'HAS_ADDRESS'
        ]
        
        # Property names for different node types
        self.properties = {
            'Product': ['id', 'description', 'type', 'group'],
            'Customer': ['id', 'name', 'category'],
            'SalesOrder': ['id', 'totalNetAmount', 'currency'],
            'BillingDocument': ['id', 'totalNetAmount', 'creationDate'],
            'BillingDocumentItem': ['id', 'material', 'quantity'],
            'SalesOrderItem': ['id', 'material', 'quantity']
        }
        
        # Templates for different query types (fixed without KeyError)
        self.templates = {
            'simple_match': [
                "Show me all {node}s",
                "List all {node}s",
                "Get all {node} records"
            ],
            'property_filter': [
                "Find {node} with {property} = '{value}'",
                "Show {node} where {property} is '{value}'"
            ],
            'relationship': [
                "Show {node1} that have {relationship} to {node2}",
                "Find {node1}s with {relationship} to {node2}s"
            ],
            'aggregation': [
                "Count number of {node}s",
                "How many {node}s are there?",
                "Show top {limit} {node}s by {property}"
            ],
            'path': [
                "Find path from {node1} to {node2}",
                "Show relationship chain between {node1} and {node2}"
            ],
            'complex': [
                "Show {node1}s with more than {count} {relationship} {node2}s",
                "Find {node1}s that have {relationship} to {node2}"
            ],
            'error': [
                "Show me all prodcts with desciptions",
                "MATCH (n) RETURN n",
                "Find products that have billing documents",
                "Show me the orders",
                "Get products where price > 100"
            ]
        }
    
    def generate_simple_match(self, idx: int) -> TestCase:
        """Generate a simple MATCH query test case."""
        node = random.choice(self.node_labels)
        template = random.choice(self.templates['simple_match'])
        query_text = template.format(node=node)
        
        return TestCase(
            id=f"SYNTH_SIMPLE_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=f"MATCH.*{node}",
            expected_node_labels=[node],
            tags=['synthetic', 'simple', node.lower()]
        )
    
    def generate_property_filter(self, idx: int) -> TestCase:
        """Generate a property filter test case."""
        node = random.choice(self.node_labels)
        if node not in self.properties or not self.properties[node]:
            return self.generate_simple_match(idx)
        
        property_name = random.choice(self.properties[node])
        sample_value = f"sample_{random.randint(1, 100)}"
        
        template = random.choice(self.templates['property_filter'])
        query_text = template.format(node=node, property=property_name, value=sample_value)
        
        return TestCase(
            id=f"SYNTH_FILTER_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=f"MATCH.*{node}.*WHERE",
            expected_node_labels=[node],
            tags=['synthetic', 'filter', node.lower()]
        )
    
    def generate_relationship_query(self, idx: int) -> TestCase:
        """Generate a relationship query test case."""
        rel = random.choice(self.relationships)
        node1 = random.choice(self.node_labels)
        node2 = random.choice(self.node_labels)
        
        template = random.choice(self.templates['relationship'])
        query_text = template.format(node1=node1, node2=node2, relationship=rel)
        
        return TestCase(
            id=f"SYNTH_REL_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=f"{rel}",
            expected_relationships=[rel],
            tags=['synthetic', 'relationship', rel.lower()]
        )
    
    def generate_aggregation_query(self, idx: int) -> TestCase:
        """Generate an aggregation query test case."""
        node = random.choice(self.node_labels)
        
        # Choose random aggregation type
        agg_type = random.choice(['count', 'top'])
        
        if agg_type == 'count':
            template = random.choice([t for t in self.templates['aggregation'] if 'Count' in t or 'count' in t])
            query_text = template.format(node=node)
            expected_pattern = r"COUNT"
        else:
            template = random.choice([t for t in self.templates['aggregation'] if 'top' in t.lower()])
            limit = random.choice([5, 10, 25])
            property_name = random.choice(self.properties.get(node, ['id']))
            query_text = template.format(node=node, limit=limit, property=property_name)
            expected_pattern = r"ORDER BY.*DESC.*LIMIT"
        
        return TestCase(
            id=f"SYNTH_AGG_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=expected_pattern,
            tags=['synthetic', 'aggregation', node.lower()]
        )
    
    def generate_path_query(self, idx: int) -> TestCase:
        """Generate a path query test case."""
        node1 = random.choice(self.node_labels)
        node2 = random.choice(self.node_labels)
        
        template = random.choice(self.templates['path'])
        query_text = template.format(node1=node1, node2=node2)
        
        return TestCase(
            id=f"SYNTH_PATH_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=r"MATCH.*path",
            tags=['synthetic', 'path', node1.lower(), node2.lower()]
        )
    
    def generate_complex_query(self, idx: int) -> TestCase:
        """Generate a complex query with multiple conditions."""
        node1 = random.choice(self.node_labels)
        node2 = random.choice(self.node_labels)
        rel = random.choice(self.relationships)
        count = random.randint(2, 10)
        
        template = random.choice(self.templates['complex'])
        query_text = template.format(
            node1=node1, 
            node2=node2, 
            relationship=rel,
            count=count
        )
        
        return TestCase(
            id=f"SYNTH_COMPLEX_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=r"MATCH.*WHERE",
            tags=['synthetic', 'complex', node1.lower()]
        )
    
    def generate_error_case(self, idx: int) -> TestCase:
        """Generate a test case that might cause errors."""
        query_text = random.choice(self.templates['error'])
        
        return TestCase(
            id=f"SYNTH_ERROR_{idx}_{random.randint(1000, 9999)}",
            natural_language=query_text,
            expected_pattern=r".*",
            should_succeed=False,
            tags=['synthetic', 'error']
        )
    
    def generate_batch(self, count: int = 100, distribution: Dict[str, float] = None) -> List[TestCase]:
        """Generate a batch of synthetic test cases."""
        if distribution is None:
            distribution = {
                'simple': 0.2,
                'filter': 0.2,
                'relationship': 0.2,
                'aggregation': 0.15,
                'path': 0.1,
                'complex': 0.1,
                'error': 0.05
            }
        
        test_cases = []
        generators = {
            'simple': self.generate_simple_match,
            'filter': self.generate_property_filter,
            'relationship': self.generate_relationship_query,
            'aggregation': self.generate_aggregation_query,
            'path': self.generate_path_query,
            'complex': self.generate_complex_query,
            'error': self.generate_error_case
        }
        
        for i in range(count):
            # Determine type based on distribution
            r = random.random()
            cumsum = 0
            selected_type = 'simple'
            for gen_type, prob in distribution.items():
                cumsum += prob
                if r < cumsum:
                    selected_type = gen_type
                    break
            
            generator = generators.get(selected_type)
            if generator:
                test_case = generator(i)
                test_cases.append(test_case)
        
        return test_cases
    
    def generate_coverage_optimized_batch(self, count: int = 100) -> List[TestCase]:
        """Generate test cases optimized for maximum coverage."""
        test_cases = []
        
        # Cover all node types
        for i, node in enumerate(self.node_labels[:10]):  # Limit to first 10
            test_cases.append(TestCase(
                id=f"COV_NODE_{node}",
                natural_language=f"Show me all {node}s",
                expected_pattern=f"MATCH.*{node}",
                expected_node_labels=[node],
                tags=['coverage', 'node', node.lower()]
            ))
        
        # Cover key relationships
        for i, rel in enumerate(self.relationships[:10]):
            node1 = random.choice(self.node_labels)
            node2 = random.choice(self.node_labels)
            test_cases.append(TestCase(
                id=f"COV_REL_{rel}",
                natural_language=f"Show {node1}s that have {rel} to {node2}s",
                expected_pattern=f"{rel}",
                expected_relationships=[rel],
                tags=['coverage', 'relationship', rel.lower()]
            ))
        
        # Add billing-product specific queries
        test_cases.append(TestCase(
            id="COV_BILLING_PRODUCT",
            natural_language="Which products appear in the most billing documents?",
            expected_pattern=r"BILLS_PRODUCT",
            expected_relationships=["BILLS_PRODUCT", "HAS_ITEM"],
            tags=['coverage', 'billing', 'product']
        ))
        
        # Limit to requested count
        return test_cases[:count]


# ============================================================================
# TELEMETRY STORAGE
# ============================================================================

class TelemetryStorage:
    """Store and retrieve telemetry data."""
    
    def __init__(self, storage_dir: str = "./telemetry"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.current_file = self.storage_dir / f"telemetry_{datetime.now().strftime('%Y%m%d')}.jsonl"
        self.summary = TelemetrySummary()
    
    def store_telemetry(self, telemetry: QueryTelemetry):
        """Store a telemetry record."""
        try:
            with open(self.current_file, 'a') as f:
                f.write(json.dumps(telemetry.to_dict(), default=str) + '\n')
            self.summary.update_from_telemetry(telemetry)
        except Exception as e:
            print(f"Error storing telemetry: {e}")


# ============================================================================
# TEST SUITE
# ============================================================================

class QueryTestSuite:
    """Test suite for validating query engine."""
    
    def __init__(self):
        self.generator = SyntheticTestCaseGenerator()
        self.test_cases: List[TestCase] = []
        self.results: List[Dict] = []
        self.telemetry = TelemetryStorage()
    
    def add_test_case(self, test_case: TestCase):
        """Add a test case to the suite."""
        self.test_cases.append(test_case)
    
    def generate_synthetic_test_cases(self, count: int = 100, 
                                      distribution: Dict[str, float] = None,
                                      coverage_optimized: bool = False) -> List[TestCase]:
        """Generate synthetic test cases."""
        if coverage_optimized:
            synthetic_cases = self.generator.generate_coverage_optimized_batch(count)
        else:
            synthetic_cases = self.generator.generate_batch(count, distribution)
        
        for case in synthetic_cases:
            self.test_cases.append(case)
        
        print(f"✅ Generated {len(synthetic_cases)} synthetic test cases")
        return synthetic_cases
    
    def validate_query(self, generated: str, expected_pattern: str) -> bool:
        """Validate generated query against expected pattern."""
        if not expected_pattern:
            return True
        try:
            return bool(re.search(expected_pattern, generated, re.IGNORECASE | re.DOTALL))
        except:
            return False
    
    def run_test_case(self, test_case: TestCase) -> Dict:
        """Run a single test case."""
        start_time = time.time()
        
        try:
            result = query(test_case.natural_language)
        except Exception as e:
            result = {
                "error": True,
                "error_type": "exception",
                "answer": str(e),
                "cypher": None,
                "results": []
            }
        
        execution_time = (time.time() - start_time) * 1000
        
        # Validate
        validation_passed = True
        validation_errors = []
        
        if test_case.expected_pattern:
            if not self.validate_query(result.get('cypher', ''), test_case.expected_pattern):
                validation_passed = False
                validation_errors.append(f"Pattern mismatch: expected {test_case.expected_pattern}")
        
        if test_case.expected_node_labels:
            for label in test_case.expected_node_labels:
                if label not in result.get('cypher', ''):
                    validation_passed = False
                    validation_errors.append(f"Missing node label: {label}")
        
        if test_case.expected_relationships:
            for rel in test_case.expected_relationships:
                if rel not in result.get('cypher', ''):
                    validation_passed = False
                    validation_errors.append(f"Missing relationship: {rel}")
        
        if test_case.should_succeed and result.get('error'):
            validation_passed = False
            validation_errors.append(f"Expected success but got error: {result.get('error_type')}")
        
        success = not result.get('error', False) and not result.get('guardrail', False)
        
        # Store telemetry
        telemetry = QueryTelemetry(
            query_id=test_case.id,
            timestamp=datetime.now(),
            user_query=test_case.natural_language,
            generated_cypher=result.get('cypher', ''),
            execution_time_ms=execution_time,
            success=success,
            error_type=result.get('error_type'),
            error_message=result.get('answer') if result.get('error') else None,
            attempts=result.get('attempts', 1),
            strategy_used=result.get('strategy_used', 'direct'),
            guardrail_triggered=result.get('guardrail', False),
            results_count=len(result.get('results', []))
        )
        self.telemetry.store_telemetry(telemetry)
        
        return {
            'test_case': test_case,
            'result': result,
            'execution_time_ms': execution_time,
            'validation_passed': validation_passed,
            'validation_errors': validation_errors,
            'success': success
        }
    
    def run_suite(self) -> Dict:
        """Run all test cases."""
        print(f"\n{'='*60}")
        print(f"Running Test Suite with {len(self.test_cases)} test cases")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        
        for i, case in enumerate(self.test_cases, 1):
            print(f"[{i}/{len(self.test_cases)}] Testing: {case.natural_language[:70]}...")
            result = self.run_test_case(case)
            self.results.append(result)
            
            status = "✅" if result['validation_passed'] else "❌"
            print(f"  {status} Success: {result['success']}, Validation: {result['validation_passed']}, Time: {result['execution_time_ms']:.0f}ms")
            if result['validation_errors']:
                print(f"     Errors: {', '.join(result['validation_errors'][:2])}")
        
        total_time = time.time() - start_time
        
        # Calculate statistics
        stats = self._calculate_statistics()
        stats['total_time_seconds'] = total_time
        
        self._print_report(stats)
        
        return stats
    
    def _calculate_statistics(self) -> Dict:
        """Calculate test suite statistics."""
        if not self.results:
            return {}
        
        total = len(self.results)
        passed = sum(1 for r in self.results if r['validation_passed'])
        query_success = sum(1 for r in self.results if r['success'])
        
        # By error type
        error_types = defaultdict(int)
        for r in self.results:
            if not r['success']:
                error_type = r['result'].get('error_type', 'unknown')
                error_types[error_type] += 1
        
        # By strategy
        strategies = defaultdict(int)
        for r in self.results:
            strategy = r['result'].get('strategy_used', 'direct')
            strategies[strategy] += 1
        
        # By tag
        tags = defaultdict(int)
        for r in self.results:
            for tag in r['test_case'].tags:
                tags[tag] += 1
        
        return {
            'total_test_cases': total,
            'validation_passed': passed,
            'validation_rate': passed / total if total > 0 else 0,
            'query_successful': query_success,
            'query_success_rate': query_success / total if total > 0 else 0,
            'error_distribution': dict(error_types),
            'strategy_distribution': dict(strategies),
            'tag_distribution': dict(tags),
            'avg_execution_time_ms': statistics.mean([r['execution_time_ms'] for r in self.results]) if self.results else 0,
            'median_execution_time_ms': statistics.median([r['execution_time_ms'] for r in self.results]) if self.results else 0,
        }
    
    def _print_report(self, stats: Dict):
        """Print test suite report."""
        print(f"\n{'='*60}")
        print("TEST SUITE REPORT")
        print(f"{'='*60}")
        print(f"\n📊 Overview:")
        print(f"   Total test cases: {stats['total_test_cases']}")
        print(f"   Validation passed: {stats['validation_passed']}/{stats['total_test_cases']} ({stats['validation_rate']*100:.1f}%)")
        print(f"   Query execution success: {stats['query_successful']}/{stats['total_test_cases']} ({stats['query_success_rate']*100:.1f}%)")
        
        print(f"\n⏱️  Performance:")
        print(f"   Total execution time: {stats['total_time_seconds']:.2f}s")
        print(f"   Avg time per query: {stats['avg_execution_time_ms']:.0f}ms")
        print(f"   Median time per query: {stats['median_execution_time_ms']:.0f}ms")
        
        if stats['error_distribution']:
            print(f"\n❌ Error Distribution:")
            for error_type, count in sorted(stats['error_distribution'].items(), key=lambda x: -x[1]):
                print(f"   {error_type}: {count} ({count/stats['total_test_cases']*100:.1f}%)")
        
        if stats['strategy_distribution']:
            print(f"\n🎯 Strategy Distribution:")
            for strategy, count in sorted(stats['strategy_distribution'].items(), key=lambda x: -x[1]):
                print(f"   {strategy}: {count} ({count/stats['total_test_cases']*100:.1f}%)")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main test execution function."""
    print("🚀 Starting Query Engine Telemetry System")
    print("="*60)
    
    if not QUERY_ENGINE_AVAILABLE:
        print("⚠️  Query engine not available. Using mock mode.")
        print("   To use real query engine, ensure llm_engine.py is in the path\n")
    
    # Create test suite
    suite = QueryTestSuite()
    
    # Generate synthetic test cases
    print("\n🔧 Generating synthetic test cases...")
    
    # Generate coverage-optimized test cases
    coverage_cases = suite.generate_synthetic_test_cases(
        count=20,
        coverage_optimized=True
    )
    
    # Generate random test cases with distribution
    random_cases = suite.generate_synthetic_test_cases(
        count=30,
        distribution={
            'simple': 0.2,
            'filter': 0.2,
            'relationship': 0.2,
            'aggregation': 0.15,
            'path': 0.1,
            'complex': 0.1,
            'error': 0.05
        }
    )
    
    print(f"\n📊 Total test cases: {len(suite.test_cases)}")
    print(f"   - Coverage-optimized: {len(coverage_cases)}")
    print(f"   - Random distribution: {len(random_cases)}")
    
    # Run test suite
    stats = suite.run_suite()
    
    # Print overall metrics
    print("\n📊 Overall Query Engine Metrics:")
    overall_metrics = get_metrics()
    print(json.dumps(overall_metrics, indent=2, default=str))
    
    # Calculate quality metrics
    if suite.results:
        precision = sum(1 for r in suite.results if r['validation_passed']) / len(suite.results)
        recall = sum(1 for r in suite.results if r['success']) / len(suite.results)
        accuracy = sum(1 for r in suite.results if r['validation_passed'] and r['success']) / len(suite.results)
        
        print(f"\n🎯 Quality Metrics (based on {len(suite.results)} tests):")
        print(f"   Precision: {precision*100:.2f}% (validation passed / total)")
        print(f"   Recall: {recall*100:.2f}% (execution success / total)")
        print(f"   Accuracy: {accuracy*100:.2f}% (both validation and execution success)")
        print(f"   Error Rate: {(1 - recall)*100:.2f}% (queries that failed)")
    
    # Save results
    if suite.results:
        results_file = Path("test_results.json")
        with open(results_file, 'w') as f:
            json.dump({
                'stats': stats,
                'results': [
                    {
                        'test_case_id': r['test_case'].id,
                        'query': r['test_case'].natural_language,
                        'success': r['success'],
                        'validation_passed': r['validation_passed'],
                        'errors': r['validation_errors'],
                        'execution_time_ms': r['execution_time_ms'],
                        'strategy': r['result'].get('strategy_used', 'direct')
                    }
                    for r in suite.results
                ]
            }, f, indent=2, default=str)
        print(f"\n💾 Results saved to {results_file}")


if __name__ == "__main__":
    main()