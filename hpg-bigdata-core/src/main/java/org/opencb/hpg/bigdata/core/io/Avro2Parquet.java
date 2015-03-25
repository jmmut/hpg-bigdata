package org.opencb.hpg.bigdata.core.io;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import parquet.avro.AvroParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

public class Avro2Parquet extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// all paths in HDFS
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf, "Avro2Parquet");		
		job.setJarByClass(getClass());

		// get Avro schema
    	String str = "{\"type\":\"record\",\"name\":\"ReadAlignment\",\"namespace\":\"org.ga4gh.models\",\"doc\":\"Each read alignment describes an alignment with additional information\\nabout the fragment and the read. A read alignment object is equivalent to a\\nline in a SAM file.\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"doc\":\"The read alignment ID. This ID is unique within the read group this\\n  alignment belongs to. This field may not be provided by all backends.\\n  Its intended use is to make caching and UI display easier for\\n  genome browsers and other light weight clients.\"},{\"name\":\"readGroupId\",\"type\":\"string\",\"doc\":\"The ID of the read group this read belongs to.\\n  (Every read must belong to exactly one read group.)\"},{\"name\":\"fragmentName\",\"type\":\"string\",\"doc\":\"The fragment name. Equivalent to QNAME (query template name) in SAM.\"},{\"name\":\"properPlacement\",\"type\":[\"boolean\",\"null\"],\"doc\":\"The orientation and the distance between reads from the fragment are\\n  consistent with the sequencing protocol (equivalent to SAM flag 0x2)\",\"default\":false},{\"name\":\"duplicateFragment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"The fragment is a PCR or optical duplicate (SAM flag 0x400)\",\"default\":false},{\"name\":\"numberReads\",\"type\":[\"null\",\"int\"],\"doc\":\"The number of reads in the fragment (extension to SAM flag 0x1)\",\"default\":null},{\"name\":\"fragmentLength\",\"type\":[\"null\",\"int\"],\"doc\":\"The observed length of the fragment, equivalent to TLEN in SAM.\",\"default\":null},{\"name\":\"readNumber\",\"type\":[\"null\",\"int\"],\"doc\":\"The read number in sequencing. 0-based and less than numberReads. This field\\n  replaces SAM flag 0x40 and 0x80.\",\"default\":null},{\"name\":\"failedVendorQualityChecks\",\"type\":[\"boolean\",\"null\"],\"doc\":\"SAM flag 0x200\",\"default\":false},{\"name\":\"alignment\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"LinearAlignment\",\"doc\":\"A linear alignment can be represented by one CIGAR string.\",\"fields\":[{\"name\":\"position\",\"type\":{\"type\":\"record\",\"name\":\"Position\",\"doc\":\"A `Position` is a side of a base pair in some already known sequence. A\\n`Position` is represented by a sequence name or ID, a base number on that\\nsequence (0-based), and a `Strand` to indicate the left or right side.\\n\\nFor example, given the sequence \\\"GTGG\\\", the `Position` on that sequence at\\noffset 1 in the forward orientation would be the left side of the T/A base pair.\\nThe base at this `Position` is \\\"T\\\". Alternately, for offset 1 in the reverse\\norientation, the `Position` would be the right side of the T/A base pair, and\\nthe base at the `Position` is \\\"A\\\".\\n\\nOffsets added to a `Position` are interpreted as reading along its strand;\\nadding to a reverse strand position actually subtracts from its `position`\\nmember.\",\"fields\":[{\"name\":\"referenceName\",\"type\":[\"null\",\"string\"],\"doc\":\"The name of the reference sequence in whatever reference set is being used.\\n  Does not generally include a \\\"chr\\\" prefix, so for example \\\"X\\\" would be used\\n  for the X chromosome.\\n\\n  If `sequenceId` is null, this must not be null.\",\"default\":null},{\"name\":\"sequenceId\",\"type\":[\"null\",\"string\"],\"doc\":\"The ID of the sequence on which the `Position` is located. This may be a\\n  `Reference` sequence, or a novel piece of sequence associated with a\\n  `VariantSet`.\\n\\n  If `referenceName` is null, this must not be null.\\n\\n  If the server supports the \\\"graph\\\" mode, this must not be null.\",\"default\":null},{\"name\":\"position\",\"type\":\"long\",\"doc\":\"The 0-based offset from the start of the forward strand for that sequence.\\n  Genomic positions are non-negative integers less than sequence length.\"},{\"name\":\"strand\",\"type\":{\"type\":\"enum\",\"name\":\"Strand\",\"doc\":\"Indicates the DNA strand associate for some data item.\\n* `POS_STRAND`:  The postive (+) strand.\\n* `NEG_STRAND`: The negative (-) strand.\\n* `NO_STRAND`: Strand-independent data or data where the strand can not be determined.\",\"symbols\":[\"POS_STRAND\",\"NEG_STRAND\",\"NO_STRAND\"]},\"doc\":\"Strand the position is associated with. `POS_STRAND` represents the forward\\n  strand, or equivalently the left side of a base, and `NEG_STRAND` represents\\n  the reverse strand, or equivalently the right side of a base.\"}]},\"doc\":\"The position of this alignment.\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"CigarUnit\",\"doc\":\"A structure for an instance of a CIGAR operation.\",\"fields\":[{\"name\":\"operation\",\"type\":{\"type\":\"enum\",\"name\":\"CigarOperation\",\"doc\":\"An enum for the different types of CIGAR alignment operations that exist.\\nUsed wherever CIGAR alignments are used. The different enumerated values\\nhave the following usage:\\n\\n* `ALIGNMENT_MATCH`: An alignment match indicates that a sequence can be\\n  aligned to the reference without evidence of an INDEL. Unlike the\\n  `SEQUENCE_MATCH` and `SEQUENCE_MISMATCH` operators, the `ALIGNMENT_MATCH`\\n  operator does not indicate whether the reference and read sequences are an\\n  exact match. This operator is equivalent to SAM's `M`.\\n* `INSERT`: The insert operator indicates that the read contains evidence of\\n  bases being inserted into the reference. This operator is equivalent to\\n  SAM's `I`.\\n* `DELETE`: The delete operator indicates that the read contains evidence of\\n  bases being deleted from the reference. This operator is equivalent to\\n  SAM's `D`.\\n* `SKIP`: The skip operator indicates that this read skips a long segment of\\n  the reference, but the bases have not been deleted. This operator is\\n  commonly used when working with RNA-seq data, where reads may skip long\\n  segments of the reference between exons. This operator is equivalent to\\n  SAM's 'N'.\\n* `CLIP_SOFT`: The soft clip operator indicates that bases at the start/end\\n  of a read have not been considered during alignment. This may occur if the\\n  majority of a read maps, except for low quality bases at the start/end of\\n  a read. This operator is equivalent to SAM's 'S'. Bases that are soft clipped\\n  will still be stored in the read.\\n* `CLIP_HARD`: The hard clip operator indicates that bases at the start/end of\\n  a read have been omitted from this alignment. This may occur if this linear\\n  alignment is part of a chimeric alignment, or if the read has been trimmed\\n  (e.g., during error correction, or to trim poly-A tails for RNA-seq). This\\n  operator is equivalent to SAM's 'H'.\\n* `PAD`: The pad operator indicates that there is padding in an alignment.\\n  This operator is equivalent to SAM's 'P'.\\n* `SEQUENCE_MATCH`: This operator indicates that this portion of the aligned\\n  sequence exactly matches the reference (e.g., all bases are equal to the\\n  reference bases). This operator is equivalent to SAM's '='.\\n* `SEQUENCE_MISMATCH`: This operator indicates that this portion of the\\n  aligned sequence is an alignment match to the reference, but a sequence\\n  mismatch (e.g., the bases are not equal to the reference). This can\\n  indicate a SNP or a read error. This operator is equivalent to SAM's 'X'.\",\"symbols\":[\"ALIGNMENT_MATCH\",\"INSERT\",\"DELETE\",\"SKIP\",\"CLIP_SOFT\",\"CLIP_HARD\",\"PAD\",\"SEQUENCE_MATCH\",\"SEQUENCE_MISMATCH\"]},\"doc\":\"The operation type.\"},{\"name\":\"operationLength\",\"type\":\"long\",\"doc\":\"The number of bases that the operation runs for.\"},{\"name\":\"referenceSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"`referenceSequence` is only used at mismatches (`SEQUENCE_MISMATCH`)\\n  and deletions (`DELETE`). Filling this field replaces the MD tag.\\n  If the relevant information is not available, leave this field as `null`.\",\"default\":null}]}},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels, etc)\\n  versus the reference.\",\"default\":[]}]},{\"type\":\"record\",\"name\":\"GraphAlignment\",\"doc\":\"A string-to-reference-graph alignment can be represented by one CIGAR string and\\none `Path` through multiple `Reference`s, against which the CIGAR string is\\ninterpreted.\\n\\nNote that `Path`s in `GraphAlignment`s are restricted to visiting `Reference`s\\nand following reference adjacencies. If a read needs to be aligned to sequences\\nthat are not present in a `ReferenceSet`, it needs to be aligned to a new\\n`ReferenceSet` with those sequences. If a read needs to follow adjacencies that\\nare not present in the `ReferenceSet` it's being aligned to, it should be\\nrepresented as a \\\"chimeric\\\" alignment, and should use multiple `ReadAlignment`s\\nand the supplementaryAlignment flag instead of a single `GraphAlignment`.\\n\\nSome especially large deletions could be represented just as well as a large\\ndeletion in the CIGAR string, or as a chimeric alignment.\",\"fields\":[{\"name\":\"path\",\"type\":{\"type\":\"record\",\"name\":\"Path\",\"doc\":\"A `Path` is an ordered list of `Segment`s. In general any contiguous path\\nthrough a sequence graph, with no novel adjacencies, can be represented by a\\n`Path`.\",\"fields\":[{\"name\":\"segments\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Segment\",\"doc\":\"A `Segment` is a range on a sequence, possibly including the joins at the\\nsequence's ends. It does not include base data. (The bases for a sequence are\\navailable through the `getSequenceBases()` API call.)\\n\\nIn the sequence \\\"GTGG\\\", the segment starting at index 1 on the forward strand\\nwith length 2 is the \\\"TG\\\" on the forward strand. The length-2 segment starting\\nat index 1 on the reverse strand is \\\"AC\\\", corresponding to the first two base\\npairs of the sequence, or the last two bases of the reverse complement.\",\"fields\":[{\"name\":\"start\",\"type\":\"Position\",\"doc\":\"The sequence ID and start index of this `Segment`. This base is always\\n  included in the segment, regardless of orientation.\"},{\"name\":\"length\",\"type\":\"long\",\"doc\":\"The length of this `Segment`'s sequence. If `start` is on the forward strand,\\n  the `Segment` contains the range [`start.position`, `start.position` +\\n  `length`). If `start` is on the reverse strand, the `Segment` contains the\\n  range (`start.position` - `length`, `start.position`]. This is equivalent to\\n  starting from the side indicated by `start`, and traversing through that base\\n  out to the specified length.\"},{\"name\":\"startJoin\",\"type\":[\"null\",\"Position\"],\"doc\":\"Start and end `Position`s where this `Segment` attaches to other sequences.\\n  Note that the segmentId for start and end might not be the same. The\\n  `Segment`s covering the sequences onto which this `Segment` is joined are\\n  called its \\\"parents\\\", while this segment is a \\\"child\\\".\\n\\n  Joins may occur on the outer sides of the terminal bases in a sequence: the\\n  left side of the base at index 0, and the right side of the base with maximum\\n  index. These are the \\\"terminal sides\\\" of the sequence. `startJoin` is the join\\n  on the side indicated by `start`, and may only be set if that side is a\\n  terminal side. Similarly, `endJoin` is the join on the opposite side of the\\n  piece of sequence selected by the segment, and may only be set if that side is\\n  a terminal side. The value of `startJoin` or `endJoin`, if set, is the side to\\n  which the corresponding side of this `Sequence` is connected.\",\"default\":null},{\"name\":\"endJoin\",\"type\":[\"null\",\"Position\"],\"default\":null}]}},\"doc\":\"We require that one of each consecutive pair of `Segment`s in a `Path` be\\n  joined onto the other. `Segment`s appear in the order in which they occur when\\n  walking the path from one end to the other.\",\"default\":[]}]},\"doc\":\"The `Path` against which the read is aligned\"},{\"name\":\"mappingQuality\",\"type\":[\"null\",\"int\"],\"doc\":\"The mapping quality of this alignment. Represents how likely\\n  the read maps to this position as opposed to other locations.\",\"default\":null},{\"name\":\"cigar\",\"type\":{\"type\":\"array\",\"items\":\"CigarUnit\"},\"doc\":\"Represents the local alignment of this sequence (alignment matches, indels,\\n  etc) versus the `Path`.\",\"default\":[]}]}],\"doc\":\"The alignment for this alignment record. This field will be\\n  null if the read is unmapped.\\n\\n  If an API server supports \\\"classic\\\" mode, it must not return `GraphAlignment`\\n  objects here. If the API server supports the \\\"graph\\\" mode and does not support\\n  the \\\"classic\\\" mode, it must not return `LinearAlignment` objects here.\",\"default\":null},{\"name\":\"secondaryAlignment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"Whether this alignment is secondary. Equivalent to SAM flag 0x100.\\n  A secondary alignment represents an alternative to the primary alignment\\n  for this read. Aligners may return secondary alignments if a read can map\\n  ambiguously to multiple coordinates in the genome.\\n\\n  By convention, each read has one and only one alignment where both\\n  secondaryAlignment and supplementaryAlignment are false.\",\"default\":false},{\"name\":\"supplementaryAlignment\",\"type\":[\"boolean\",\"null\"],\"doc\":\"Whether this alignment is supplementary. Equivalent to SAM flag 0x800.\\n  Supplementary alignments are used in the representation of a chimeric\\n  alignment, which follows nonreference adjacencies not describable as indels.\\n  In a chimeric alignment, a read is split into multiple alignments that\\n  may map to different reference contigs. The first alignment in the read will\\n  be designated as the representative alignment; the remaining alignments will\\n  be designated as supplementary alignments. These alignments may have different\\n  mapping quality scores.\\n\\n  In each alignment in a chimeric alignment, the read will be hard clipped. The\\n  `alignedSequence` and `alignedQuality` fields in the alignment record will\\n  only represent the bases for its respective alignment.\",\"default\":false},{\"name\":\"alignedSequence\",\"type\":[\"null\",\"string\"],\"doc\":\"The bases of the read sequence contained in this alignment record.\\n  `alignedSequence` and `alignedQuality` may be shorter than the full read sequence\\n  and quality. This will occur if the alignment is part of a chimeric alignment,\\n  or if the read was trimmed. When this occurs, the CIGAR for this read will\\n  begin/end with a hard clip operator that will indicate the length of the excised sequence.\",\"default\":null},{\"name\":\"alignedQuality\",\"type\":{\"type\":\"array\",\"items\":\"int\"},\"doc\":\"The quality of the read sequence contained in this alignment record.\\n  `alignedSequence` and `alignedQuality` may be shorter than the full read sequence\\n  and quality. This will occur if the alignment is part of a chimeric alignment,\\n  or if the read was trimmed. When this occurs, the CIGAR for this read will\\n  begin/end with a hard clip operator that will indicate the length of the excised sequence.\",\"default\":[]},{\"name\":\"nextMatePosition\",\"type\":[\"null\",\"Position\"],\"doc\":\"The mapping of the primary alignment of the `(readNumber+1)%numberReads`\\n  read in the fragment. It replaces mate position and mate strand in SAM.\",\"default\":null},{\"name\":\"info\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"string\"}},\"doc\":\"A map of additional read alignment information.\",\"default\":{}}]}"; 
    	Schema schema = new org.apache.avro.Schema.Parser().parse(str); //ReadAlignment.getClassSchema();
		//Schema schema = Read.getClassSchema(); //ReadAlignment.getClassSchema();
//		Schema schema = ReadAlignment.getClassSchema();
//		System.out.println(new AvroSchemaConverter().convert(schema).toString());

		// point to input data
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// set the output format
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, schema);
		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		AvroParquetOutputFormat.setCompressOutput(job, true);

		// set a large block size to ensure a single row group.  see discussion
		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

		job.setMapperClass(Avro2ParquetMapper.class);
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}