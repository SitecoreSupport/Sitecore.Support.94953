using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Sitecore.ContentSearch;

namespace Sitecore.Support.ContentSearch.Azure
{
  public class CloudSearchDocumentBuilder : Sitecore.ContentSearch.Azure.CloudSearchDocumentBuilder
  {

    public CloudSearchDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }


    protected override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.Indexable.LoadAllFields();
        }

        if (IsParallel)
        {
          var exceptions = new ConcurrentQueue<Exception>();
          Parallel.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
          {
            try
            {
              this.CheckAndAddField(this.Indexable, f);
            }
            catch (Exception ex)
            {
              exceptions.Enqueue(ex);
            }
          });

          if (exceptions.Count > 0)
          {
            throw new AggregateException(exceptions);
          }
        }
        else
        {
          foreach (var field in this.Indexable.Fields)
          {
            this.CheckAndAddField(this.Indexable, field);
          }
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }
  }
}