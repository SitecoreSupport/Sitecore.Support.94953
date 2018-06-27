
namespace Sitecore.Support.ContentSearch.Azure
{
  using System;
  using System.Collections.Concurrent;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Diagnostics;
  using StringExtensions;

  public class CloudSearchDocumentBuilder : Sitecore.ContentSearch.Azure.CloudSearchDocumentBuilder
  {

    public CloudSearchDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }

    protected override void AddField(IIndexableDataField field)
    {
      if (field.Name.IsNullOrEmpty())
      {
        CrawlingLog.Log.Warn("[Index={0}] '{1}' field of '{2}' item is skipped: the field name is missed."
          .FormatWith(this.Index.Name, field.Id, this.Indexable.Id));

        return;
      }

      base.AddField(field);
    }

    protected override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.AddItemFieldsByItemList();
        }
        else
        {
          this.AddItemFieldsByIncludeList();
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }

    protected virtual void AddItemFieldsByItemList()
    {

    }

    protected virtual void AddItemFieldsByIncludeList()
    {
      
    }
  }
}