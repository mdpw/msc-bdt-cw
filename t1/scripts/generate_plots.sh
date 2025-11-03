#!/usr/bin/env python3

import matplotlib.pyplot as plt
import numpy as np
import os
import glob
import argparse
from pathlib import Path

def find_result_files(base_dir, dataset_name):
    """Automatically find Hadoop and Spark result files for any dataset"""
    base_path = Path(os.path.expanduser(base_dir))
    
    # Find Hadoop results for the specified dataset
    hadoop_patterns = [
        f"{dataset_name}/hadoop/output-*/part-r-00000",
        f"{dataset_name}/hadoop/*/part-r-00000",
        f"hadoop/{dataset_name}/output-*/part-r-00000"
    ]
    
    # Find Spark result directories (not individual files)
    spark_patterns = [
        f"{dataset_name}/spark/final-output-*",
        f"{dataset_name}/spark/output-*", 
        f"{dataset_name}/spark/*",
        f"spark/{dataset_name}/output-*"
    ]
    
    hadoop_files = []
    spark_dirs = []
    
    for pattern in hadoop_patterns:
        hadoop_files.extend(glob.glob(str(base_path / pattern)))
    
    for pattern in spark_patterns:
        potential_dirs = glob.glob(str(base_path / pattern))
        # Filter to only directories that contain part files
        for dir_path in potential_dirs:
            if os.path.isdir(dir_path) and glob.glob(os.path.join(dir_path, "part-*")):
                spark_dirs.append(dir_path)
    
    return hadoop_files, spark_dirs

def parse_hadoop_file(file_path):
    """Parse Hadoop output file"""
    degrees = []
    counts = []
    
    print(f"Reading: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                    
                try:
                    # Hadoop format: degree\tcount
                    parts = line.replace('\t', ' ').split()
                    if len(parts) >= 2:
                        degree = int(parts[0])
                        count = int(parts[1])
                        degrees.append(degree)
                        counts.append(count)
                except (ValueError, IndexError):
                    continue
                    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return [], []
    
    print(f"Parsed {len(degrees)} data points")
    return degrees, counts

def parse_spark_directory(dir_path):
    """Parse all Spark part files in a directory"""
    degrees = []
    counts = []
    
    print(f"Reading Spark directory: {dir_path}")
    
    # Find all part files
    part_files = sorted(glob.glob(os.path.join(dir_path, "part-*")))
    part_files = [f for f in part_files if not f.endswith('.crc') and not f.endswith('_SUCCESS')]
    
    print(f"Found {len(part_files)} part files")
    
    for part_file in part_files:
        try:
            with open(part_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                        
                    try:
                        # Spark format: (degree, count)
                        if line.startswith('(') and line.endswith(')'):
                            line = line[1:-1]  # Remove parentheses
                            parts = line.split(',')
                            degree = int(parts[0].strip())
                            count = int(parts[1].strip())
                            degrees.append(degree)
                            counts.append(count)
                    except (ValueError, IndexError):
                        continue
                        
        except Exception as e:
            print(f"Error reading {part_file}: {e}")
            continue
    
    print(f"Parsed {len(degrees)} total data points from all part files")
    return degrees, counts

def create_comparison_plots(hadoop_data, spark_data, dataset_name, output_dir="./"):
    """Create comparison plots for Hadoop vs Spark results for any dataset"""
    
    hadoop_degrees, hadoop_counts = hadoop_data
    spark_degrees, spark_counts = spark_data
    
    # Check what data we have
    has_hadoop = len(hadoop_degrees) > 0
    has_spark = len(spark_degrees) > 0
    
    if not has_hadoop and not has_spark:
        print("No valid data to plot!")
        return None, None
    
    # Convert to numpy arrays and sort
    if has_hadoop:
        h_deg, h_cnt = np.array(hadoop_degrees), np.array(hadoop_counts)
        h_sorted = np.argsort(h_deg)
        h_deg, h_cnt = h_deg[h_sorted], h_cnt[h_sorted]
    
    if has_spark:
        s_deg, s_cnt = np.array(spark_degrees), np.array(spark_counts)
        s_sorted = np.argsort(s_deg)
        s_deg, s_cnt = s_deg[s_sorted], s_cnt[s_sorted]
    
    # Create comprehensive comparison plot
    fig = plt.figure(figsize=(20, 16))
    
    # Dynamic title based on dataset and available data
    if has_hadoop and has_spark:
        title = f'{dataset_name.upper()} Dataset - In-Degree Distribution Analysis\nHadoop MapReduce vs Apache Spark Comparison'
    elif has_hadoop:
        title = f'{dataset_name.upper()} Dataset - In-Degree Distribution Analysis\nHadoop MapReduce Results'
    else:
        title = f'{dataset_name.upper()} Dataset - In-Degree Distribution Analysis\nApache Spark Results'
    
    fig.suptitle(title, fontsize=18, fontweight='bold', y=0.95)
    
    # Create grid layout
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    # 1. Linear Scale Comparison
    ax1 = fig.add_subplot(gs[0, 0])
    if has_hadoop:
        ax1.scatter(h_deg, h_cnt, alpha=0.7, s=30, color='blue', label='Hadoop MapReduce', edgecolors='darkblue')
    if has_spark:
        ax1.scatter(s_deg, s_cnt, alpha=0.7, s=30, color='red', label='Apache Spark', marker='o')
    ax1.set_xlabel('In-Degree')
    ax1.set_ylabel('Number of Nodes')
    ax1.set_title('Linear Scale Distribution', fontweight='bold')
    if has_hadoop and has_spark:
        ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Log-Log Scale Comparison
    ax2 = fig.add_subplot(gs[0, 1])
    
    # Use available data for power law analysis
    primary_deg = h_deg if has_hadoop else s_deg
    primary_cnt = h_cnt if has_hadoop else s_cnt
    
    # Filter positive values for log scale
    if has_hadoop:
        h_pos = (h_deg > 0) & (h_cnt > 0)
        ax2.loglog(h_deg[h_pos], h_cnt[h_pos], 'o', alpha=0.7, markersize=5, color='blue', label='Hadoop')
    if has_spark:
        s_pos = (s_deg > 0) & (s_cnt > 0)
        ax2.loglog(s_deg[s_pos], s_cnt[s_pos], 's', alpha=0.7, markersize=5, color='red', label='Spark')
    
    ax2.set_xlabel('In-Degree (log scale)')
    ax2.set_ylabel('Number of Nodes (log scale)')
    ax2.set_title('Log-Log Scale (Power Law)', fontweight='bold')
    if has_hadoop and has_spark:
        ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Add power law fit
    pos_mask = (primary_deg > 0) & (primary_cnt > 0)
    if np.sum(pos_mask) > 1:
        log_x = np.log10(primary_deg[pos_mask])
        log_y = np.log10(primary_cnt[pos_mask])
        coeffs = np.polyfit(log_x, log_y, 1)
        slope = coeffs[0]
        
        trend_x = np.logspace(np.log10(min(primary_deg[pos_mask])), np.log10(max(primary_deg[pos_mask])), 50)
        trend_y = 10**(coeffs[1]) * trend_x**slope
        ax2.plot(trend_x, trend_y, '--', color='green', linewidth=2, 
                label=f'Power Law: gamma={slope:.2f}')
        ax2.legend()
    
    # 3. Semi-Log Scale
    ax3 = fig.add_subplot(gs[0, 2])
    if has_hadoop:
        ax3.semilogy(h_deg, h_cnt, 'o-', alpha=0.7, markersize=4, color='blue', label='Hadoop')
    if has_spark:
        ax3.semilogy(s_deg, s_cnt, 's-', alpha=0.7, markersize=4, color='red', label='Spark')
    ax3.set_xlabel('In-Degree')
    ax3.set_ylabel('Number of Nodes (log scale)')
    ax3.set_title('Semi-Log Distribution', fontweight='bold')
    if has_hadoop and has_spark:
        ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Difference Analysis or Single System View
    ax4 = fig.add_subplot(gs[1, :])
    
    if has_hadoop and has_spark:
        # Find common degrees for comparison
        common_degrees = np.intersect1d(h_deg, s_deg)
        differences = []
        
        for deg in common_degrees:
            h_idx = np.where(h_deg == deg)[0]
            s_idx = np.where(s_deg == deg)[0]
            if len(h_idx) > 0 and len(s_idx) > 0:
                diff = h_cnt[h_idx[0]] - s_cnt[s_idx[0]]
                differences.append(diff)
            else:
                differences.append(0)
        
        ax4.bar(common_degrees, differences, alpha=0.7, color='purple', edgecolor='black')
        ax4.axhline(y=0, color='red', linestyle='--', linewidth=2)
        ax4.set_xlabel('In-Degree')
        ax4.set_ylabel('Difference (Hadoop - Spark)')
        ax4.set_title('Result Verification: Difference Analysis (Should be ~0 for identical results)', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add statistics text
        max_diff = max(abs(d) for d in differences) if differences else 0
        avg_diff = np.mean(np.abs(differences)) if differences else 0
        
        ax4.text(0.02, 0.95, f'Max Absolute Difference: {max_diff}\nAverage Absolute Difference: {avg_diff:.2f}', 
                 transform=ax4.transAxes, fontsize=12,
                 bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.8),
                 verticalalignment='top')
    else:
        # Show single system distribution
        primary_system = "Hadoop" if has_hadoop else "Spark"
        ax4.bar(primary_deg, primary_cnt, alpha=0.7, color='blue' if has_hadoop else 'red', edgecolor='black')
        ax4.set_xlabel('In-Degree')
        ax4.set_ylabel('Number of Nodes')
        ax4.set_title(f'{primary_system} Results - Full Distribution View', fontweight='bold')
        ax4.grid(True, alpha=0.3)
    
    # 5. Statistics Table
    ax5 = fig.add_subplot(gs[2, :])
    ax5.axis('off')
    
    # Calculate statistics for available data
    stats_data = [['Metric', 'Hadoop MapReduce', 'Apache Spark', 'Match']]
    
    if has_hadoop:
        h_total_nodes = sum(hadoop_counts)
        h_total_edges = sum(d * c for d, c in zip(hadoop_degrees, hadoop_counts))
        h_avg_degree = h_total_edges / h_total_nodes if h_total_nodes > 0 else 0
        h_max_degree = max(hadoop_degrees) if hadoop_degrees else 0
    else:
        h_total_nodes = h_total_edges = h_avg_degree = h_max_degree = 0
    
    if has_spark:
        s_total_nodes = sum(spark_counts)
        s_total_edges = sum(d * c for d, c in zip(spark_degrees, spark_counts))
        s_avg_degree = s_total_edges / s_total_nodes if s_total_nodes > 0 else 0
        s_max_degree = max(spark_degrees) if spark_degrees else 0
    else:
        s_total_nodes = s_total_edges = s_avg_degree = s_max_degree = 0
    
    # Add rows to stats table
    stats_data.extend([
        ['Total Nodes', f'{h_total_nodes:,}' if has_hadoop else 'N/A', 
         f'{s_total_nodes:,}' if has_spark else 'N/A', 
         'YES' if has_hadoop and has_spark and h_total_nodes == s_total_nodes else 'N/A'],
        ['Total Edges', f'{h_total_edges:,}' if has_hadoop else 'N/A', 
         f'{s_total_edges:,}' if has_spark else 'N/A', 
         'YES' if has_hadoop and has_spark and h_total_edges == s_total_edges else 'N/A'],
        ['Average In-Degree', f'{h_avg_degree:.2f}' if has_hadoop else 'N/A', 
         f'{s_avg_degree:.2f}' if has_spark else 'N/A', 
         'YES' if has_hadoop and has_spark and abs(h_avg_degree - s_avg_degree) < 0.01 else 'N/A'],
        ['Max In-Degree', f'{h_max_degree:,}' if has_hadoop else 'N/A', 
         f'{s_max_degree:,}' if has_spark else 'N/A', 
         'YES' if has_hadoop and has_spark and h_max_degree == s_max_degree else 'N/A'],
        ['Distribution Entries', f'{len(hadoop_degrees)}' if has_hadoop else 'N/A', 
         f'{len(spark_degrees)}' if has_spark else 'N/A', 
         'YES' if has_hadoop and has_spark and len(hadoop_degrees) == len(spark_degrees) else 'N/A']
    ])
    
    table = ax5.table(cellText=stats_data[1:], colLabels=stats_data[0], 
                     cellLoc='center', loc='center',
                     colWidths=[0.3, 0.2, 0.2, 0.1])
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2)
    
    # Style the table
    for i in range(len(stats_data)):
        for j in range(len(stats_data[0])):
            cell = table[(i, j)]
            if i == 0:  # Header
                cell.set_facecolor('#4CAF50')
                cell.set_text_props(weight='bold', color='white')
            elif j == 3:  # Match column
                if i > 0 and stats_data[i][j] == 'YES':
                    cell.set_facecolor('#E8F5E8')
                elif i > 0 and stats_data[i][j] == 'NO':
                    cell.set_facecolor('#FFE8E8')
    
    ax5.set_title('Computational Correctness Verification', fontweight='bold', fontsize=14, pad=20)
    
    plt.tight_layout()
    
    # Save the plot
    output_path = Path(output_dir) / f"{dataset_name}_hadoop_vs_spark_comparison.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Plot saved: {output_path}")
    
    return fig, output_path

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Generate in-degree distribution plots for any dataset')
    parser.add_argument('dataset', help='Dataset name (e.g., pokec, facebook, twitter)')
    parser.add_argument('--results-dir', default='~/bigdata-assignment/results', 
                       help='Base results directory (default: ~/bigdata-assignment/results)')
    parser.add_argument('--output-dir', default='./plots', 
                       help='Output directory for plots (default: ./plots)')
    
    args = parser.parse_args()
    
    dataset_name = args.dataset.lower()
    results_dir = args.results_dir
    output_dir = Path(args.output_dir)
    
    print(f"Generating plots for dataset: {dataset_name.upper()}")
    print(f"Searching in: {results_dir}")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find result files
    hadoop_files, spark_dirs = find_result_files(results_dir, dataset_name)
    
    print(f"\nFound files:")
    print(f"   Hadoop files: {len(hadoop_files)}")
    for f in hadoop_files:
        print(f"     - {f}")
    print(f"   Spark directories: {len(spark_dirs)}")
    for d in spark_dirs:
        print(f"     - {d}")
    
    if not hadoop_files and not spark_dirs:
        print(f"No result files found for dataset '{dataset_name}'!")
        return
    
    # Use the most recent files
    hadoop_file = hadoop_files[-1] if hadoop_files else None
    spark_dir = spark_dirs[-1] if spark_dirs else None
    
    print(f"\nProcessing files:")
    
    hadoop_data = ([], [])
    spark_data = ([], [])
    
    if hadoop_file:
        print(f"   Hadoop: {hadoop_file}")
        hadoop_data = parse_hadoop_file(hadoop_file)
    
    if spark_dir:
        print(f"   Spark: {spark_dir}")
        spark_data = parse_spark_directory(spark_dir)
    
    # Generate plots
    print(f"\nCreating distribution plots...")
    fig, plot_path = create_comparison_plots(hadoop_data, spark_data, dataset_name, output_dir)
    
    if plot_path:
        print(f"\nSUCCESS! Plot saved to: {plot_path}")
        print(f"Output directory: {output_dir}")
        
        # Show statistics
        hadoop_degrees, hadoop_counts = hadoop_data
        spark_degrees, spark_counts = spark_data
        
        print(f"\n=== {dataset_name.upper()} ANALYSIS SUMMARY ===")
        if hadoop_degrees:
            h_total = sum(hadoop_counts)
            h_edges = sum(d * c for d, c in zip(hadoop_degrees, hadoop_counts))
            print(f"Hadoop: {h_total:,} nodes, {h_edges:,} edges")
        
        if spark_degrees:
            s_total = sum(spark_counts)
            s_edges = sum(d * c for d, c in zip(spark_degrees, spark_counts))
            print(f"Spark: {s_total:,} nodes, {s_edges:,} edges")
        
        # Verification
        if hadoop_degrees and spark_degrees:
            if hadoop_degrees == spark_degrees and hadoop_counts == spark_counts:
                print("Results are IDENTICAL - Perfect computational correctness!")
            else:
                print("Results differ - Check for processing differences")
        
        print(f"\nUse this plot in your assignment to demonstrate:")
        print(f"   - Power-law distribution analysis")
        print(f"   - Network topology insights")
        print(f"   - Result verification between systems")
        print(f"   - Professional visualization")

if __name__ == "__main__":
    main()