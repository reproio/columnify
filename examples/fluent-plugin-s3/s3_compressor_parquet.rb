module Fluent::Plugin
  class S3Output
    class ParquetCompressor < Compressor
      S3Output.register_compressor('parquet', self)

      config_section :compress, multi: false do
        config_param :schema_type, :string
        config_param :schema_file, :string
        config_param :record_type, :string
      end

      def configure(conf)
        super
        check_command('columnify', '-h')
      end	

      def ext
        'parquet'.freeze
      end

      def content_type
        'application/octet-stream'.freeze
      end

      def compress(chunk, tmp)
        if @buffer_type == 'file'
          columnify(@compress.schema_type, @compress.schema_file, @compress.record_type, chunk.path, tmp.path)
        else
          Tempfile.create("chunk-parquet-tmp") { |w|
            w.binmode
            chunk.write_to(w)
            w.close
            columnify(@compress.schema_type, @compress.schema_file, @compress.record_type, w.path, tmp.path)
          }
        end
      end

      private

      def columnify(schema_type, schema_file, record_type, src_path, dst_path)
        system "columnify -schemaType #{schema_type} -schemaFile #{schema_file} -recordType #{record_type} #{src_path} > #{dst_path}"
      end
    end
  end
end
